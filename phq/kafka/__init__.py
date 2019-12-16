import sys
from .calc_ref_latencies import calc_ref_latencies
from .consumer import Consumer, get_kafka_consumer
from .producer import get_kafka_producer, produce, produce_batch, Producer
from .payload import format_kafka_ref, unpack_kafka_payload, pack_kafka_payload, Message
from confluent_kafka import KafkaException

# Allow higher recursion as some messages have very long hists,
# and can't be parsed with the normal limit.
# This us usually due to something going wrong causing a cycle
# of messages between services.
# The long_hist func is now used to detect hists that have
# grown too long so they can be truncated, so hopefully these
# problematic hists aren't produced in the future.
sys.setrecursionlimit(1100)
