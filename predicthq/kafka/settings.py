KAFKA_CONSUMER_BASE_CONFIGURATION = {
    'auto.offset.reset': 'latest',
    'fetch.max.bytes': '1048576',
    'request.timeout.ms': '305000',
    'heartbeat.interval.ms': '3000',
    'session.timeout.ms': '10000',
    'max.poll.interval.ms': '300000',
    'queued.max.messages.kbytes': '100000',  # Internal queue size in Kbytes which determine the size of the internal buffer
    'enable.auto.commit': 'true'
}

KAFKA_PRODUCER_BASE_CONFIGURATION = {
    'compression.type': 'snappy',
    'acks': -1,
    'retries': 3
}
