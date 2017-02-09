from trivup import trivup
from trivup.apps.KafkaBrokerApp import KafkaBrokerApp
import os

class ZookeeperApp (trivup.App):
    """ Zookeeper server app """

    def __init__(self, cluster, conf=None, on=None, bin_path=None):
        """
        @param cluster     Current cluster
        @param on          Node name to run on

        Config:
          bindir    Path to zookeeper-server-start.sh directory (optional)
                    Falls back to Kafka bindir

        Exposes 'address' (host:port) for other apps.
        """
        super(ZookeeperApp, self).__init__(cluster, conf=conf, on=on)
        self.conf['port'] = trivup.TcpPortAllocator(self.cluster).next(self)
        self.conf['datadir'] = self.create_dir('datadir')
        self.conf['address'] = '%(nodename)s:%(port)d' % self.conf
        # Generate config file
        self.conf['conf_file'] = self.create_file_from_template('zookeeper.properties',
                                                           self.conf)

    def start_cmd(self):
        bindir = self.get('bindir', None)
        if bindir is None:
            k = self.cluster.find_app(KafkaBrokerApp)
            if k is not None:
                bindir = k.get('bindir', None)

        zkbin = 'zookeeper-server-start.sh'
        if bindir:
            zkbin = os.path.join(bindir, zkbin)

        return '%s %s' % (zkbin, self.conf['conf_file'])

    def operational (self):
        self.dbg('Checking if operational')
        return os.system('(echo stat | nc %s | grep -q Zookeeper.version) 2>/dev/null' %
                         ' '.join(self.get('address').split(':'))) == 0

    
    def deploy (self):
        """ Deploy is a no-op for ZK since it is run from Kafka dir """
        pass
