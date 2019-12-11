import logging
from typing import List, Dict

import confluent_kafka

from .consumer import PhqKafkaConsumer, Message
from .producer import PhqKafkaProducer

log = logging.getLogger(__name__)


class PhqKafkaProcessor(PhqKafkaConsumer):
    def __init__(
        self, svc: str, input_topic: str, output_topic: str,
        group_id: str, batch_size: int, consumer_timeout_ms: int, kafka_bootstrap_servers: List[str],
        kafka_consumer_config: Dict[str, str] = None, kafka_producer_config: Dict[str, str] = None
    ):
        super().__init__(input_topic, group_id, batch_size, consumer_timeout_ms, kafka_bootstrap_servers, kafka_consumer_config)
        self.producer = PhqKafkaProducer(svc, output_topic, kafka_bootstrap_servers, kafka_producer_config)

    def process(self):
        self._process(producer=self.producer)

    def process_messages(self, messages: List[Message]) -> List[Message]:
        raise NotImplementedError("subclass should implement `{}`".format('process_messages'))
