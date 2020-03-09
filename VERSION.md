## Version 0.1.14
* NEW: Debug log in consumer when committing offset and closing the consumer. 

## Version 0.1.13
* CHANGE: Introduce latest_only parameter in Consumer.process() to ignore older version of Kafka messages that appear multiple times in a single batch. 

## Version 0.1.12
* FIX: Consumer now attempts to reuse the existing Prometheus metrics that have been previously created instead of keep creating new ones. 

## Version 0.1.11
Initial release.
* FIX: fix setup.py build, add missing file in MANIFEST.in
