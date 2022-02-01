#!/usr/bin/env python

from trivup.clusters.KafkaCluster import KafkaCluster

def test_KafkaCluster():

    cluster = KafkaCluster()

    print('Starting cluster')
    cluster.start()

    print('Waiting for brokers to come up')
    cluster.wait_operational(30)

    print('Cluster started')
    cluster.stop()
