trivup - Trivially Up a cluster of applications
===============================================

trivup is a flexible, pluggable, light-weight, partly naiivistic, framework
for trivially bringing up a cluster of applications.

The initial use-case is to bring up a simple Kafka cluster for easily testing
[librdkafka](https://github.com/edenhill/librdkafka) on different
broker versions.

**NOTE**: It currently only operates on localhost.


See tests/use-case.py for a small showcase.
