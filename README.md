# trivup - Trivially Up a cluster of applications


![librdkafka tests](https://github.com/edenhill/trivup/raw/master/.librdkafka_tests.png)
*(Serving suggestion)*

trivup is a flexible, pluggable, light-weight, partly naiivistic, framework
for trivially bringing up a cluster of applications.

The initial use-case is to bring up a simple Kafka cluster for easily testing
[librdkafka](https://github.com/edenhill/librdkafka) on different
broker versions.

**NOTE**: It currently only operates on localhost.

**SECURITY WARNING**: trivup will run unprotected, wide-open, poorly-configured,
                      server applications on your machine, providing an
                      ingress vector for intrusion, theft and data-loss.
                      DO NOT RUN ON PUBLIC NETWORKS.


## Command-line example


To spin up a Kafka cluster with Confluent Schema-Registry:

    $ python -m trivup.clusters.KafkaCluster --sr

Pass `--help` for more options.

A sub-shell will be started with access to all cluster components, try:

    $ $KAFKA_PATH/bin/kafka-topics.sh --zookeeper $ZK_ADDRESS \
      --create --topic test --partitions 4 --replication-factor 3
    $ kafkacat -b $BROKERS -L

As you exit the sub-shell the cluster will be brought down and deleted:

    $ exit


## Code example

See [tests/usecase.py](tests/usecase.py) for a code example.


## Requirements

 * Python packages: `pip install -r requirements.txt`
 * Java JRE
 * Netcat
 * For SSL: openssl
 * For GSSAPI/Kerberos: krb5-kdc (linux only, will not work on osx).
 * For Schema-Registry: docker

To bootstrap your Ubuntu/Debian system with the required packages, do:

    $ make bootstrap-ubuntu
    # or, to also install krb5-kdc and docker:
    $ make bootstrap-ubuntu-full


## Cache

Set TRIVUP_ROOT=~/trivup-cache (or wherever you like) to define where
trivup will have its working directory and where downloads are cached.


