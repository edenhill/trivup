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

import uuid
import requests
import subprocess


class SchemaRegistryApp (trivup.App):
    """ Confluent Schema Registry app.
        Depends on KafkaBrokerApp.
        Requires docker. """

    default_image = 'confluentinc/cp-schema-registry:5.2.1'

    def __init__(self, cluster, conf=None, on=None):
        """
        @param cluster     Current cluster
        @param conf        Configuration dict, see below.
        @param on          Node name to run on

        Supported conf keys:
           * version - Confluent Platform version to use.
           * port_base - Low TCP port base to start allocating from (random)
           * image - docker image to use
           * conf - schema-registry docker image config strings (NOT USED)
        """
        super(SchemaRegistryApp, self).__init__(cluster, conf=conf, on=on)

        if self.conf.get('image', '') == '':
            self.conf['image'] = self.default_image

        self.conf['container_id'] = 'trivup_sr_%s' % str(uuid.uuid4())[0:7]
        kafka = cluster.find_app(KafkaBrokerApp)
        if kafka is None:
            raise Exception('KafkaBrokerApp required')

        bootstrap_servers = kafka.conf.get('docker_advertised_listeners')

        if bootstrap_servers is None:
            raise Exception('KafkaBrokerApp required')

        # Create listener
        port = trivup.TcpPortAllocator(self.cluster).next(
            self, self.conf.get('port_base', None))

        docker_args = ''
        if cluster.platform == 'linux':
            # Let container bind to host localhost
            self.conf['extport'] = port
            self.conf['intport'] = port
            docker_args = '--network=host'

        elif cluster.platform == 'darwin':
            # On OSX localhost binds are not possible, so set up a
            # port forwarding.
            self.conf['extport'] = port
            self.conf['intport'] = 8081
            docker_args = '-p %d:%d' % (self.conf['extport'],
                                        self.conf['intport'])

        # This is the listener address inside the docker container
        self.conf['listeners'] = 'http://0.0.0.0:%d' % self.conf.get('intport')
        # This is the listener address outside the docker container,
        # using port-forwarding
        self.conf['url'] = 'http://localhost:%d' % self.conf['extport']

        # Run in foreground.
        self.conf['start_cmd'] = 'docker run %s --name %s -e SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS=%s  -e SCHEMA_REGISTRY_HOST_NAME=localhost   -e SCHEMA_REGISTRY_LISTENERS=%s  -e SCHEMA_REGISTRY_DEBUG=true %s' % (  # noqa: E501
            docker_args,
            self.conf.get('container_id'),
            bootstrap_servers,
            self.conf.get('listeners'),
            self.conf.get('image'))

        # Stop through docker
        self.conf['stop_cmd'] = 'docker stop %s' % \
                                self.conf.get('container_id')

    def operational(self):
        self.dbg('Checking if %s is operational' % self.get('url'))
        try:
            r = requests.head(self.get('url'), timeout=1.0)
            if r.status_code >= 200 and r.status_code < 300:
                return True
            raise Exception('status_code %d' % r.status_code)
        except Exception as e:
            self.dbg('%s check failed: %s' % (self.get('url'), e))
            return False

    def deploy(self):
        image = self.conf.get('image')
        self.dbg('Pulling docker image: %s' % image)
        subprocess.check_call('(docker images -q "%s" 2>/dev/null | grep -q ^.) || docker pull %s' % (image, image), shell=True)  # noqa: E501
        pass
