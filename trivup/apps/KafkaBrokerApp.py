from trivup import trivup
import os
import time


class KafkaBrokerApp (trivup.App):
    """ Kafka broker app
        Depends on ZookeeperApp """
    def __init__(self, cluster, conf=None, on=None, kafka_path=None):
        """
        @param cluster     Current cluster
        @param kafka_path  Path to Kafka build tree
        @param on          Node name to run on
        """
        super(KafkaBrokerApp, self).__init__(cluster, conf=conf, on=on)

        self.zk = cluster.find_app('ZookeeperApp')
        if self.zk is None:
            raise Exception('ZookeeperApp required')

        # Kafka repo uses SVN nomenclature
        if self.conf['version'] == 'master':
            self.conf['version'] = 'trunk'

        self.appid = trivup.TcpPortAllocator(self.cluster).next()
        self.conf['port'] = self.appid
        self.conf['address'] = '%s:%d' % (self.node.name, self.conf['port'])
        self.conf['kafka_path'] = kafka_path

        # Kafka Configuration properties
        self.conf['listeners'] = 'PLAINTEXT://:%d' % self.conf['port']
        self.conf['log_dirs'] = self.create_dir('logs')
        if 'num_partitions' not in self.conf:
            self.conf['num_partitions'] = 3
        self.conf['zk_connect'] = self.zk.get('address', None)
        if 'replication_factor' not in self.conf:
            self.conf['replication_factor'] = 1
        # Generate config file
        self.conf['conf_file'] = self.create_file_from_template('server.properties',
                                                           self.conf)
        # Runs in foreground, stopped by Ctrl-C
        if kafka_path:
            start_sh = os.path.join(kafka_path, 'bin', 'kafka-server-start.sh')
        else:
            start_sh = 'kafka-server-start.sh'

        self.conf['start_cmd'] = '%s %s' % (start_sh, self.conf['conf_file'])
        self.conf['stop_cmd'] = None # Ctrl-C

    def operational (self):
        self.log('Checking if operational')
        return os.system('(echo anything | nc %s) 2>/dev/null' %
                         ' '.join(self.get('address').split(':'))) == 0


    def deploy (self):
        self.log('Deploy %s version %s on %s' %
                 (self.name, self.get('version'), self.node.name))
        print(type(self))
        deploy_exec = self.resource_path('deploy.sh')
        if not os.path.exists(deploy_exec):
            raise NotImplementedError('Kafka deploy.sh script missing in %s' %
                                      deploy_exec)
        t_start = time.time()
        cmd = '%s %s %s' % (deploy_exec, self.get('version'), self.get('kafka_path'))
        r = os.system(cmd)
        if r != 0:
            raise Exception('Deploy "%s" returned exit code %d' % (cmd, r))
        self.log('Deployed version %s in %ds' %
                 (self.get('version'), time.time() - t_start))
