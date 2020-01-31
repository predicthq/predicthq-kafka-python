from prometheus_client import Counter, Summary

CONSUME_TIME_METRIC = None
CONSUME_COUNT_METRIC = None


def consumer_time_metric(svc_name):
    # Prometheus library throws "Duplicated {metric} in CollectorRegistry" exception when
    # metric object is re-created with the same name, hence we need to keep reusing the same one.
    global CONSUME_TIME_METRIC

    if not CONSUME_TIME_METRIC:
        CONSUME_TIME_METRIC = Summary(f'{svc_name}_kafka_process_seconds', 'Time spent processing Kafka messages')

    return CONSUME_TIME_METRIC


def consumer_count_metric(svc_name):
    # Prometheus library throws "Duplicated {metric} in CollectorRegistry" exception when
    # metric object is re-created with the same name, hence we need to keep reusing the same one.
    global CONSUME_COUNT_METRIC

    if not CONSUME_COUNT_METRIC:
        CONSUME_COUNT_METRIC = Counter(f'{svc_name}_kafka_process_count', 'Count of Kafka messages processed')

    return CONSUME_COUNT_METRIC
