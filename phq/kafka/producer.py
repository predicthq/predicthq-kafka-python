import logging
from typing import List, Dict
from functools import partial

import confluent_kafka

from .payload import pack_kafka_payload, Message
from .settings import producer_base_configuration

log = logging.getLogger(__name__)


def _on_delivery_error_handler(err, message, ignore_large_message_errors=False):
    if err:
        if err.code() == confluent_kafka.KafkaError.MSG_SIZE_TOO_LARGE and not ignore_large_message_errors:
            raise confluent_kafka.KafkaException(err)
        elif err.code() == confluent_kafka.KafkaError.MSG_SIZE_TOO_LARGE:
            log.error('Tried to send message that was too large. Skipped.')
        elif err.code() != confluent_kafka.KafkaError.MSG_SIZE_TOO_LARGE:
            raise confluent_kafka.KafkaException(err)


def _produce(producer: confluent_kafka.Producer, topic: str, partition: str = None, key: str = None, value: str = None):
    # will need to see how to handle error, especially large_message_errors
    on_delivery = partial(_on_delivery_error_handler, ignore_large_message_errors=False)

    data = {
        'topic': topic,
        'key': key.encode('utf-8') if key else None,
        'value': value.encode('utf-8') if value else None,
        'on_delivery': on_delivery
    }

    if partition:
        data['partition'] = partition

    producer.produce(**data)


def produce_batch(producer: confluent_kafka.Producer, topic: str, batch: List[Dict]):
    for message in batch:
        _produce(producer, topic, message.get('partition'), message.get('key'), message.get('value'))
    producer.flush()


def produce(producer: confluent_kafka.Producer, topic: str, partition: str = None, key: str = None, value: str = None):
    _produce(producer, topic, partition, key, value)
    producer.flush()


def get_kafka_producer(bootstrap_servers: List[str], kafka_custom_config: Dict[str, str] = None) -> confluent_kafka.Producer:
    configuration = producer_base_configuration()

    if kafka_custom_config:
        configuration.update(kafka_custom_config)

    configuration['bootstrap.servers'] = ','.join(bootstrap_servers)
    return confluent_kafka.Producer(configuration, logger=log)


class Producer:
    def __init__(self, svc: str, kafka_bootstrap_servers: List[str],
                 kafka_producer_config: Dict[str, str] = None):

        self._svc_pack_kafka_payload = partial(pack_kafka_payload, svc)
        self._producer = get_kafka_producer(kafka_bootstrap_servers, kafka_producer_config)

    def produce_batch(self, output_topic: str, messages: List[Message]):
        if not output_topic:
            raise ValueError(f'Invalid Kafka output topic name: {output_topic}')

        batch = [
            {'key': _id, 'value': self._svc_pack_kafka_payload(payload, refs)}
            for _id, payload, refs in messages
        ]
        produce_batch(self._producer, output_topic, batch)

    def produce(self, output_topic, message: Message):
        if not output_topic:
            raise ValueError(f'Invalid Kafka output topic name: {output_topic}')

        _id, payload, refs = message
        packed_payload = self._svc_pack_kafka_payload(payload, refs)
        produce(self._producer, output_topic, key=_id, value=packed_payload)
