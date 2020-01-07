from prometheus_client import Counter, Summary


def consumer_time_metric(svc_name):
    return Summary(f'{svc_name}_kafka_process_seconds', 'Time spent processing Kafka messages')


def consumer_count_metric(svc_name):
    return Counter(f'{svc_name}_kafka_process_count', 'Count of Kafka messages processed')
