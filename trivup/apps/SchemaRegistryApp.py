from trivup import trivup
from trivup.apps.KafkaBrokerApp import KafkaBrokerApp

import contextlib
import os
import socket
import time
import uuid
import requests


CP_VERSION='5.2.1'  # Confluent Platform version

class SchemaRegistryApp (trivup.App):
    """ Confluent Schema Registry app.
        Depends on KafkaBrokerApp.
        Requires docker. """
    def __init__(self, cluster, conf=None, on=None):
        """
        @param cluster     Current cluster
        @param conf        Configuration dict, see below.
        @param on          Node name to run on

        Supported conf keys:
           * version - Confluent Platform version to use.
           * port_base - Low TCP port base to start allocating from (random)
           * conf - schema-registry docker image config strings.
        """
        super(SchemaRegistryApp, self).__init__(cluster, conf=conf, on=on)

        self.conf['container_id'] = 'trivup_sr_%s' % str(uuid.uuid4())[0:7]
        kafka = cluster.find_app(KafkaBrokerApp)
        print kafka
        if kafka is None:
            raise Exception('KafkaBrokerApp required')

        bootstrap_servers = kafka.conf.get('docker_advertised_listeners')

        if bootstrap_servers is None:
            raise Exception('KafkaBrokerApp required')

        # Create listener
        port = trivup.TcpPortAllocator(self.cluster).next(self, self.conf.get('port_base', None))
        self.conf['extport'] = port
        self.conf['intport'] = 8081
        # This is the listener address inside the docker container
        self.conf['listeners'] = 'http://0.0.0.0:%d' % self.conf.get('intport')
        # This is the listener address outside the docker container, using port-forwarding
        self.conf['url'] = 'http://localhost:%d' % port

        # Runs in foreground, stopped by Ctrl-C
        # This is the default for no-deploy use: will be overwritten by deploy() if enabled.

        self.conf['start_cmd'] = 'docker run --name %s -p %d:%d -e SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS=%s  -e SCHEMA_REGISTRY_HOST_NAME=localhost   -e SCHEMA_REGISTRY_LISTENERS=%s  -e SCHEMA_REGISTRY_DEBUG=true confluentinc/cp-schema-registry:%s' % (self.conf.get('container_id'), self.conf.get('extport'), self.conf.get('intport'), bootstrap_servers, self.conf.get('listeners'), CP_VERSION)
        self.conf['stop_cmd'] = 'docker stop %s' % self.conf.get('container_id')

        print(self.conf['start_cmd'])

    def operational (self):
        self.dbg('Checking if %s is operational' % self.get('url'))
        try:
            r = requests.head(self.get('url'), timeout=1.0)
            if r.status_code >= 200 and r.status_code < 300:
                return True
            raise Exception('status_code %d' % r.status_code)
        except Exception, e:
            self.dbg('%s check failed: %s' % (self.get('url'), e))
            return False

    def deploy (self):
        pass
