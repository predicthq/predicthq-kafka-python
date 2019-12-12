import logging
import itertools
import time
from collections import namedtuple
from typing import List, Dict

import confluent_kafka

from .settings import KAFKA_CONSUMER_BASE_CONFIGURATION
from .payload import unpack_kafka_payload, Message


log = log = logging.getLogger(__name__)


Tp = namedtuple('Tp', ['topic', 'partition'])


def get_kafka_consumer(bootstrap_servers: List[str], group_id: str, kafka_custom_config: Dict[str, str]) -> confluent_kafka.Consumer:
    configuration = KAFKA_CONSUMER_BASE_CONFIGURATION.copy()
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
            tp = Tp(topic, partition)
            if tp not in batch_ref:
                batch_ref[tp] = []
            batch_ref[tp] += list(m_partition)
    return batch_ref


class Consumer(object):
    def __init__(
        self, kafka_bootstrap_servers: List[str], input_topic: str, group_id: str,
            batch_size: int, consumer_timeout_ms: int, **kafka_consumer_config
    ):
        self.closed = False

        self._input_topic = input_topic
        self._consumer_group = group_id

        self._consumer_timeout_ms = consumer_timeout_ms
        self._consumer_batch_size = batch_size

        self._consumer = get_kafka_consumer(
            kafka_bootstrap_servers,
            self._consumer_group,
            kafka_consumer_config
        )

        self._consumer.subscribe([self._input_topic])

    def _format_message_batch(self, kafka_messages):
        messages = []
        batch_ref = {}
        for tp, batch in _group_messages(kafka_messages).items():
            topic = tp.topic
            partition = tp.partition

            if topic == self._input_topic:
                if topic not in batch_ref:
                    batch_ref[topic] = {}

                for message in batch:
                    # Skip null messages - they are used to 'delete' keys from
                    # compacted topics.
                    if message.value() is None:
                        continue

                    if message.error():
                        error = message.error()
                        if error.code() == confluent_kafka.KafkaError._PARTITION_EOF:
                            continue
                        else:
                            raise confluent_kafka.KafkaException(msg.error())

                    payload, ref = unpack_kafka_payload(message)
                    message_id = ref['key']
                    messages.append(Message(message_id, payload, ref))

                batch_ref[topic][partition] = (batch[0].offset(), batch[-1].offset())

            else:
                log.warning('Received %(batch)s messages for unknown topic %(unknown_topic)s (expected %(expected_topic)s). Skipping.',
                            {'batch': len(batch), 'unknown_topic': topic, 'expected_topics': self.input_topics})
                continue

        return messages, batch_ref

    def _get_kafka_messages(self):
        while True:
            # The consumer interface doesn't obey the 'consumer_timeout_ms' consumer config.
            log.debug('Calling consumer.consume, timeout %(time_s)s',
                      {'time_s': self._consumer_timeout_ms})
            messages = self._consumer.consume(num_messages=self._consumer_batch_size, timeout=self._consumer_timeout_ms / 1000)
            if messages:
                yield messages

    def close(self):
        if not self.closed:
            self._consumer.close()
            self.closed = True

    def process(self, func_handler):
        if not callable(func_handler):
            raise ValueError(f'Function handler is expected but got {type(func_handler)}')

        log.info('Starting processor.')

        messages_processed = 0

        for kafka_messages in self._get_kafka_messages():
            messages, batch_ref = self._format_message_batch(kafka_messages)
            batch_size = len(messages)
            start_ms = time.time() * 1000
            log.info('[%(batch_ref)s] Starting processing batch of %(batch)d messages.',
                     {'batch_ref': format_batch_ref(batch_ref), 'batch': batch_size})

            try:
                self.process_messages(messages)

            except Exception as e:
                log.exception('[%(batch_ref)s] Encountered exception while processing batch of messages.',
                              {'batch_ref': format_batch_ref(batch_ref)})
                raise
            else:
                self._consumer.commit()

            messages_processed += batch_size

            if batch_size:
                end_ms = time.time() * 1000
                time_ms = end_ms - start_ms
                log.info('Processed batch of %(batch)d messages in %(time_ms)dms. %(total)d total.',
                         {'batch': batch_size, 'time_ms': time_ms, 'total': messages_processed})

        if messages_processed > 0:
            log.info('Processed %(total)d messages', {'total': messages_processed})
