# PHQ kafka python library

The purpose of this library is to facilitate kafka's integration to new services.

It's a wrapper around [confluent-python-kafka](https://github.com/confluentinc/confluent-kafka-python) which is a wrapper around [librdkafka](https://github.com/edenhill/librdkafka).

This wrapper is producing and consuming json payload only.


## Note

This library wrapper is using `enable.auto.commit : false`, to control when and why we want to commit messages, it is currently commiting after a batch was processed successfully. If any error is raised during processing, the current batch will not be committed.

See [Kafka Consumer Config doc](https://docs.confluent.io/current/installation/configuration/consumer-configs.html) for more information about all the available Kafka consumer settings.


## Prerequisites

### Snappy compression

This library uses Snappy to compress messages before sending them to a Kafka topic.

Install snappy:

```bash
$ apt-get install -y libsnappy-dev
```

Or on MacOSX:

```bash
$ brew install snappy
```


## Getting started

### Producer

In some cases you may only need to produce messages, you can then use the `Producer` class:

```python
from phq.kafka import Producer, Message

if __name__ == '__name__':
    # init methods can also accept one extra parameter: kafka_producer_config.
    producer = Producer('my-service-name', ['kafka:9092'])

    my_message_batch = [Message("some_id", {"data": "my message"}, [])]
    # the last parameters of a message "refs", is use to track the lifecycle of a particular message.
    producer.produce_batch(my_message_batch)
```


## Consumer

If you only need a consumer:

```python
from phq.kafka import Consumer, Message

batch_size = 100
consumer_timeout_ms = 1000
kafka_bootstrap_server = ['kafka:9092']


def process_messages(messages):
    output_msgs = []
    for message in messages:
        print(message.id)
        print(message.payload)  # this is a dict
        output_msgs.append(message)


my_consumer = Consumer('my_svc_name', ['kafka:9092'], 'input-topic', 'group_id', batch_size, consumer_timeout_ms)
my_consumer.process(process_messages)
```

In a situation when multiple versions of the same message are being processed within the same batch, the consumer `process()` can be tweaked to process only the last version of the message and ignore the older ones by setting the optional `latest_only` parameter to `True`.

```python
my_consumer = Consumer('my_svc_name', ['kafka:9092'], 'input-topic', 'group_id', batch_size, consumer_timeout_ms)
my_consumer.process(process_messages, latest_only=True)
```

## Exception

To import and catch exceptions thrown by `confluent_kafka`, simply import KafkaException:

```python
from phq.kafka import KafkaException
```

## Configuration

This library provides a default configuration for producer and consumer available here:
[settings.py](predicthq/kafka/settings.py)

Consumer using `confluent-kafka-python` will need to have a certain amount of internal memory dedicated to a buffer which work as an internal queue. Basically, librdkafka will pre-fetch kafka messages, which will make it faster for the consumer to process the next batch.

Each configuration key can be overiden using `kafka_consumer_config` and `kafka_producer_config` arguments.

For more tunning, here is all available configuration parameters which can be used:
[librdkafka CONFIGURATION.md](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md).


## Advanced usage

To implement a really specific consumer/producer logic, some low level functions are available:

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


## Versioning

This repo follows strict semantic versioning rules. If you not familiar with them, please check [semver.org](https://semver.org/).

In a nutshell, given a version number MAJOR.MINOR.PATCH, increment the:
- MAJOR version when you make incompatible API changes,
- MINOR version when you add functionality in a backwards compatible manner, and
- PATCH version when you make backwards compatible bug fixes.

