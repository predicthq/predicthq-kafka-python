# PredictHQ kafka python library

The purpose of this library is to make easier the integration of kafka to a new services, and to propagate every related change to the way and configuration of kafka easily.

It's a wrapper around [confluent-python-kafka](https://github.com/confluentinc/confluent-kafka-python) which is a wrapper arround [librdkafka](https://github.com/edenhill/librdkafka).

To understand the choice of `confluent-python-kafka` over `kafka-python` please read this wiki page:
[https://predicthq.atlassian.net/wiki/spaces/DEV/pages/48857662/Python+Kafka+Library+Investigation]

## Getting started

To implement a class processor which consume and produce messages you can use the `PhqKafkaProcessor` class:

```python
from predicthq.kafka import PhqKafkaProcessor, Message

class CustomProcessor(PhqKafkaProcessor):

    def process_messages(self, messages: List[Message]) -> List[Message]:
        for message in messages:
            print(message.id)
            print(message.payload) # this is a dict
        return [Message("some_id", {"data": "my message"}, [])]

if __name__ == "__name__":

    batch_size = 100
    consumer_timeout_ms = 1000
    kafka_bootstrap_server = ['kafka:9092']

    consumer = CustomProcessor(
        "my-service-name", "input-topic", "output-topic",
        "group_id", batch_size, consumer_timeout_ms, ['kafka:9092']
    ) # init methods can also accept two extra parameter: kafka_consumer_config, kafka_producer_config

    # to start processing:
    consumer.process()

    # you can also produce message to another topic if necessary:
    my_message_batch = [Message("some_other_id", {"data": "my message"}, [])]
    consumer.produce_batch(my_message_batch, "other-topic")

```

Each class inheriting from PhqKafkaProducer must have a process_messages methods which will handle the received messages and return a list of new Message object.

## Using only a producer

In some cases you may only need to produce message, you can then use the `PhqKafkaProducer` class:

```python
from predicthq.kafka import PhqKafkaProducer, Message

if __name__ == '__name__':
    batch_size = 100
    consumer_timeout_ms = 1000
    kafka_bootstrap_server = ['kafka:9092']

    consumer = PhqKafkaProducer(
        'my-service-name', 'output-topic', ['kafka:9092']
    ) # init methods can also accept one extra parameter: kafka_producer_config.

    my_message_batch = [Message("some_id", {"data": "my message"}, [])]
    consumer.produce_batch(my_message_batch)
```

## Using only a consumer

In several case, we will only need to consume message (like in the event-submitter services), the code in this case will look a lot like the one for KafkaPhqProcessor:

```python
from predicthq.kafka import PhqKafkaConsumer, Message

class CustomConsumer(PhqKafkaConsumer):

    def process_messages(self, messages: List[Message]):
        for message in messages:
            print(message.id)
            print(message.payload) # this is a dict

if __name__ == "__name__":

    batch_size = 100
    consumer_timeout_ms = 1000
    kafka_bootstrap_server = ['kafka:9092']

    consumer = CustomConsumer(
        "input-topic", "group_id",
        batch_size, consumer_timeout_ms, ['kafka:9092']
    ) # init methods can also accept an extra parameter: kafka_consumer_config.

    # to start processing:
    consumer.process()
```

## Exception

To import and catch exception throw by `confluent_kafka`, simply import KafkaException:

```python
from predicthq.kafka import KafkaException
```

## Configuration

This library provide a default configuration for producer and consumer available here:
[settings.py](predicthq/kafka/settings.py)

Consumer using `confluent-kafka-python` will need to have a certain amount of internal memorry dedicated to a buffer which work as an internal queue.
Basically, librdkafka will pre-fetch kafka messages, which will make it faster for the consumer to process the next batch.

Each configuration key can be overiden using `kafka_consumer_config` and `kafka_producer_config` arguments.

For more tunning, here is all available configuration parameters which can be used:
[librdkafka CONFIGURATION.md](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md).

# Advance usage

To implement a really specific consumer/producer logic, some low level function are available:

```python
def get_kafka_consumer(bootstrap_servers: List[str], group_id: str, kafka_custom_config: Dict[str, str]) -> confluent_kafka.Consumer: pass

def format_batch_ref(batch_ref): pass

def get_kafka_producer(bootstrap_servers: List[str], kafka_custom_config: Dict[str, str] = None) -> confluent_kafka.Producer: pass

def produce(producer: confluent_kafka.Producer, topic: str, partition: str = None, key: str = None, value: str = None): pass

def produce_batch(producer: confluent_kafka.Producer, topic: str, batch: str, ignore_large_message_errors=False): pass

def format_kafka_ref(ref): pass

def unpack_kafka_payload(message): pass

def pack_kafka_payload(svc, item, refs=[]): pass
```

## Note

This library wrapper is using `enable.auto.commit : false`, to control when and why we want to commit messages, it is currently commiting after the successfull processing of a batch.
If any error is raised during processing, the current batch will not be commited.
