import logging
import json
from typing import List, Dict
from functools import partial

import confluent_kafka

from .payload import pack_kafka_payload, Message
from .settings import KAFKA_PRODUCER_BASE_CONFIGURATION

log = logging.getLogger(__name__)


def _on_delivery_error_handler(err, message, ignore_large_message_errors=False):
    if err:
        if err.code() == confluent_kafka.KafkaError.MSG_SIZE_TOO_LARGE and not ignore_large_message_errors:
            raise confluent_kafka.KafkaException(err)
        elif err.code() == confluent_kafka.KafkaError.MSG_SIZE_TOO_LARGE:
            log.error('Tried to send message that was too large. Skipped.')
        elif err.code() != confluent_kafka.KafkaError.MSG_SIZE_TOO_LARGE:
            raise confluent_kafka.KafkaException(err)


def produce_batch(producer: confluent_kafka.Producer, topic: str, batch: str, ignore_large_message_errors=False):
    # will need to see how to handle error, especially large_messsage_errors
    on_delivery = partial(_on_delivery_error_handler, ignore_large_message_errors=ignore_large_message_errors)
    for message in batch:
        data = {
            'topic': topic,
            'key': message['key'].encode('utf-8') if message.get('key') else None,
            'value': json.dumps(message['value']).encode('utf-8') if message.get('value') else None,
            'on_delivery': on_delivery
        }
        if message.get('partition'):
            data['partition'] = message['partition']
        producer.produce(**data)

    producer.flush()


def produce(producer: confluent_kafka.Producer, topic: str, partition: str = None, key: str = None, value: str = None):
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
    producer.flush()


def get_kafka_producer(bootstrap_servers: List[str], kafka_custom_config: Dict[str, str] = None) -> confluent_kafka.Producer:
    configuration = KAFKA_PRODUCER_BASE_CONFIGURATION.copy()
    if kafka_custom_config:
        configuration.update(kafka_custom_config)
    configuration.update({
        'bootstrap.servers': ",".join(bootstrap_servers)
    })
    return confluent_kafka.Producer(configuration, logger=log)


class Producer(object):
    def __init__(
        self, svc: str, output_topic: str,
        kafka_bootstrap_servers: List[str], kafka_producer_config: Dict[str, str] = None
    ):
        self.svc = svc

        self.output_topic = output_topic

        self._producer = get_kafka_producer(
            kafka_bootstrap_servers,
            kafka_producer_config
        )

        self._svc_pack_kafka_payload = partial(pack_kafka_payload, self.svc)

    def produce_batch(self, messages: List[Message], output_topic=None):
        batch = []
        for _id, payload, ref in messages:
            packed_payload = self._svc_pack_kafka_payload(payload, [ref])
            batch.append({'key': _id, 'value': packed_payload})
        produce_batch(
            self._producer,
            output_topic if output_topic else self.output_topic,
            batch
        )

    def produce(self, message: Message, output_topic=None):
        _id, payload, ref = messages
        packed_payload = self._svc_pack_kafka_payload(payload, [ref])
        produce(
            self._producer,
            output_topic if output_topic else self.output_topic,
            key=_id, value=pack_kafka_payload
        )