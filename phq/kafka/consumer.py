import logging
import itertools
import time
from collections import namedtuple, OrderedDict
from typing import List, Dict, Any, Union

import confluent_kafka

from .settings import consumer_base_configuration
from .payload import unpack_kafka_payload, Message
from .metrics import consumer_time_metric, consumer_count_metric

log = logging.getLogger(__name__)


TopicPartition = namedtuple('TopicPartition', ['topic', 'partition'])


def get_kafka_consumer(bootstrap_servers: List[str], group_id: str, kafka_custom_config: Dict[str, str]) -> confluent_kafka.Consumer:
    configuration = consumer_base_configuration()

    if kafka_custom_config:
        configuration.update(kafka_custom_config)

    configuration.update({
        'bootstrap.servers': ",".join(bootstrap_servers),
        'group.id': group_id,
    })
    return confluent_kafka.Consumer(configuration, logger=log)


def format_batch_ref(batch_ref):
    return ','.join('{}[{}]'.format(topic, ','.join('{}={}:{}'.format(partition, min_offset, max_offset + 1)
                                                    for partition, (min_offset, max_offset)
                                                    in partition_dict.items()))
                    for topic, partition_dict in batch_ref.items())


def _group_messages(messages):
    batch_ref = {}
    for topic, m_topic in itertools.groupby(messages, lambda x: x.topic()):
        for partition, m_partition in itertools.groupby(m_topic, lambda x: x.partition()):
            tp = TopicPartition(topic, partition)
            if tp not in batch_ref:
                batch_ref[tp] = []
            batch_ref[tp] += list(m_partition)
    return batch_ref


def _latest_distinct_messages(messages):
    latest_distinct_msgs = OrderedDict()
    for msg in messages[::-1]:
        if msg.id not in latest_distinct_msgs:
            latest_distinct_msgs[msg.id] = msg
    return list(latest_distinct_msgs.values())[::-1]


class Consumer(object):
    def __init__(self, svc_name: str, kafka_bootstrap_servers: List[str], input_topics: Union[str, List[str]], consumer_group: str,
                 batch_size: int, consumer_timeout_ms: int, commit_message: bool = True, kafka_consumer_config: Dict[str, Any] = None):

        self.metrics = {
            'CONSUME_TIME': consumer_time_metric(svc_name),
            'CONSUME_COUNT': consumer_count_metric(svc_name)
        }

        self.closed = False

        # use to accept either a single topic or a list of topics
        self._input_topics = input_topics if isinstance(input_topics, (list,)) else [input_topics]
        self._consumer_group = consumer_group
        self._auto_commit = commit_message

        if kafka_consumer_config:
            kafka_consumer_config = kafka_consumer_config.copy()
            if 'enable.auto.commit' in kafka_consumer_config:
                log.warning("'enable.auto.commit' option is not supported by 'phq-python-kafka' library, see the documentation")
                del kafka_consumer_config['enable.auto.commit']

        self._consumer_timeout_ms = consumer_timeout_ms
        self._consumer_batch_size = batch_size

        self._consumer = get_kafka_consumer(kafka_bootstrap_servers,
                                            self._consumer_group,
                                            kafka_consumer_config)

        self._consumer.subscribe(self._input_topics)

    def _format_message_batch(self, kafka_messages):
        messages = []
        batch_ref = {}
        for tp, batch in _group_messages(kafka_messages).items():
            topic = tp.topic
            partition = tp.partition

            if not topic or topic in self._input_topics:
                if topic not in batch_ref:
                    batch_ref[topic] = {}

                for message in batch:
                    # Skip null messages - they are used to 'delete' keys from
                    # compacted topics.
                    if message.value() is None:
                        continue

                    if message.error():
                        error = message.error()
                        # this is not really an error, we may have reach the end of the topic, we shouldn't raise and exception or stop consuming,
                        # and keep polling until another batch is available.
                        if error.code() == confluent_kafka.KafkaError._PARTITION_EOF:
                            continue
                        else:
                            raise confluent_kafka.KafkaException(message.error())

                    payload, ref = unpack_kafka_payload(message)
                    message_id = ref['key']
                    messages.append(Message(message_id, payload, ref, topic))

                batch_ref[topic][partition] = (batch[0].offset(), batch[-1].offset())

            else:
                log.warning('Received %(batch)s messages for unknown topic %(unknown_topic)s (expected %(expected_topics)s). Skipping.',
                            {'batch': len(batch), 'unknown_topic': topic, 'expected_topics': self._input_topics})
                continue

        return messages, batch_ref

    def _get_kafka_messages(self):
        while True:
            log.debug('Calling consumer.consume, timeout %(time_s)s',
                      {'time_s': self._consumer_timeout_ms})
            # try to consume num_messages as soon as possible, if num_messages isn't reach until timeout, then it will consume what's available.
            messages = self._consumer.consume(num_messages=self._consumer_batch_size, timeout=self._consumer_timeout_ms / 1000)
            if messages:
                yield messages

    def close(self):
        if not self.closed:
            log.debug('Closing Kafka consumer.')
            self._consumer.close()
            self.closed = True

    def process(self, func_handler, latest_only=False):
        if not callable(func_handler):
            raise ValueError(f'Function handler is expected but got {type(func_handler)}')

        log.info('Starting Kafka consumer.')

        messages_processed = 0

        for kafka_messages in self._get_kafka_messages():
            messages, batch_ref = self._format_message_batch(kafka_messages)
            batch_size = len(messages)
            start_ms = time.time() * 1000
            log.debug('[%(batch_ref)s] Starting processing batch of %(batch)d messages.',
                      {'batch_ref': format_batch_ref(batch_ref), 'batch': batch_size})

            start_s = time.perf_counter()
            try:
                if latest_only:
                    messages = _latest_distinct_messages(messages)

                func_handler(_latest_distinct_messages(messages) if latest_only else messages)
            except Exception as e:
                log.exception('[%(batch_ref)s] Encountered exception while processing batch of messages.',
                              {'batch_ref': format_batch_ref(batch_ref)})
                raise
            else:
                if self._auto_commit:
                    log.debug('Committing Kafka offset.')
                    self._consumer.commit()

            self.metrics['CONSUME_TIME'].observe(time.perf_counter() - start_s)
            self.metrics['CONSUME_COUNT'].inc(len(messages))

            messages_processed += batch_size

            if batch_size:
                end_ms = time.time() * 1000
                time_ms = end_ms - start_ms
                log.debug('Processed batch of %(batch)d messages in %(time_ms)dms. %(total)d total.',
                         {'batch': batch_size, 'time_ms': time_ms, 'total': messages_processed})

        if messages_processed > 0:
            log.debug('Processed %(total)d messages', {'total': messages_processed})
