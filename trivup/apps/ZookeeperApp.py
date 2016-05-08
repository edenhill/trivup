from trivup import trivup
import os

class ZookeeperApp (trivup.App):
    """ Zookeeper server app """

    def __init__(self, cluster, conf=None, on=None, bin_path=None):
        """
        @param cluster     Current cluster
        @param on          Node name to run on
        @param bin_path    Path to zookeeper-server-start.sh

        Exposes 'address' (host:port) for other apps.
        """
        super(ZookeeperApp, self).__init__(cluster, conf=conf, on=on)
        self.conf['port'] = trivup.TcpPortAllocator(self.cluster).next()
        self.conf['datadir'] = self.create_dir('datadir')
        self.conf['address'] = '%(nodename)s:%(port)d' % self.conf
        # Generate config file
        self.conf['conf_file'] = self.create_file_from_template('zookeeper.properties',
                                                           self.conf)

        if bin_path:
            self.conf['start_cmd'] = '%s %s' % (bin_path, self.conf['conf_file'])

    def operational (self):
        self.log('Checking if operational')
        return os.system('(echo stat | nc %s | grep -q Zookeeper.version) 2>/dev/null' %
                         ' '.join(self.get('address').split(':'))) == 0

    
    def deploy (self):
        self.log('Deploy is a no-op for ZK since it is run from Kafka dir')
