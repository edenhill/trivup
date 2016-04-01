#!/usr/bin/env python

from trivup.trivup import Cluster
from trivup.apps.ZookeeperApp import ZookeeperApp
from trivup.apps.KafkaBrokerApp import KafkaBrokerApp

import subprocess
import time


if __name__ == '__main__':
    cluster = Cluster('TestCluster', 'tmp')

    # One ZK
    zk1 = ZookeeperApp(cluster, bin_path='/home/maglun/src/kafka/bin/zookeeper-server-start.sh')

    # Two brokers
    conf = {'replication_factor': 3, 'num_partitions': 4}
    broker1 = KafkaBrokerApp(cluster, conf, kafka_path='/home/maglun/src/kafka')
    broker2 = KafkaBrokerApp(cluster, conf, kafka_path='/home/maglun/src/kafka')
    broker3 = KafkaBrokerApp(cluster, conf, kafka_path='/home/maglun/src/kafka')
    bootstrap_servers = ','.join(cluster.get_all('address','',KafkaBrokerApp))

    print('# Deploying cluster')
    cluster.deploy()

    print('# Starting cluster')
    cluster.start()

    print('# Waiting for brokers to come up')

    if not cluster.wait_operational(30):
        print('# Cluster did not go operational: letting you troubleshoot in shell')
    print('# Connect to cluster with bootstrap.servers %s' % bootstrap_servers)
        
    print('\033[32mCluster started.. Executing interactive shell, exit to stop cluster\033[0m')
    subprocess.call("bash", shell=True)

    cluster.stop(force=True)

    cluster.cleanup(keeptypes=['perm','log'])

