from phq.kafka.consumer import _latest_distinct_messages
from phq.kafka import Message


def test_latest_distinct_messages():
    messages = [
        Message(id='abc', payload={}),
        Message(id='def', payload={}),
        Message(id='xyz', payload={}),
        Message(id='xyz', payload={}),
        Message(id='abc', payload={}),
    ]

    distinct_messages = _latest_distinct_messages(messages)
    assert len(distinct_messages) == 3
    assert distinct_messages[0] is messages[1]
    assert distinct_messages[1] is messages[3]
    assert distinct_messages[2] is messages[4]
