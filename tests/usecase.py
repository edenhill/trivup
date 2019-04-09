#!/usr/bin/env python

from trivup.trivup import Cluster
from trivup.apps.ZookeeperApp import ZookeeperApp
from trivup.apps.KafkaBrokerApp import KafkaBrokerApp

import subprocess
import time
import os

# Directory for cloning Apache Kafka sources to.
# It will only be used if 'version' is not specified.
# Otherwise Apache Kafka binary distrubution will be downloaded to
# a cluster data dir ( trivup/tmp/TestCluster/KafkaBrokerApp/kafka/conf['version'] )
kafka_dir=os.path.join(os.path.expanduser('~'), "src", "kafka")

if __name__ == '__main__':
    cluster = Cluster('TestCluster', 'tmp')

    # One ZK
    zk1 = ZookeeperApp(cluster, bin_path=os.path.join(kafka_dir, '/bin/zookeeper-server-start.sh'))

    # Multiple Kafka brokers
    conf = {'version': '2.2.0', 'replication_factor': 3, 'num_partitions': 4, 'kafka_path': kafka_dir}
    broker1 = KafkaBrokerApp(cluster, conf)
    broker2 = KafkaBrokerApp(cluster, conf)
    broker3 = KafkaBrokerApp(cluster, conf)
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

