#!/usr/bin/env python
#

# Copyright (c) 2016-2019, Magnus Edenhill
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# * Redistributions of source code must retain the above copyright notice, this
#   list of conditions and the following disclaimer.
#
# * Redistributions in binary form must reproduce the above copyright notice,
#   this list of conditions and the following disclaimer in the documentation
#   and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED.
# IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

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
          zk_port   Port at which Zookeeper should be bound (optional)
                    A (random) free port will be chosen otherwise

        Exposes 'address' (host:port) for other apps.
        """
        super(ZookeeperApp, self).__init__(cluster, conf=conf, on=on)
        self.conf['port'] = trivup.TcpPortAllocator(self.cluster).next(self, 
                                                                       port_base=self.conf.get('zk_port', None))
        self.conf['datadir'] = self.create_dir('datadir')
        self.conf['address'] = '%(nodename)s:%(port)d' % self.conf
        # Generate config file
        self.conf['conf_file'] = self.create_file_from_template('zookeeper.properties', self.conf)  # noqa: E501

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

    def operational(self):
        self.dbg('Checking if operational')
        return os.system(('(echo srvr | nc %s | grep -q Zookeeper.version) '
                          '2>/dev/null') %
                         ' '.join(self.get('address').split(':'))) == 0

    def deploy(self):
        """ Deploy is a no-op for ZK since it is run from Kafka dir """
        pass
