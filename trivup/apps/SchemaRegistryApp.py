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
from trivup.apps.SslApp import SslApp

import requests


sr_image = 'confluentinc/cp-schema-registry'


class SchemaRegistryApp (trivup.DockerApp):
    """ Confluent Schema Registry app.
        Depends on KafkaBrokerApp.
        Requires docker. """

    default_conf = {
        'debug': True,
        'host_name': 'localhost',
        'listeners': 'http://0.0.0.0:8081'
    }

    default_version = '5.3.0'

    def __init__(self, cluster, conf=None, on='docker'):
        """
        @param cluster     Current cluster
        @param conf        Configuration dict, see below.
        @param on          Node name to run on

        Supported conf keys:
           * version - Confluent Platform version to use.
           * port_base - Low TCP port base to start allocating from (random)
           * conf - Confluent Schema Registry configs
        """

        app_conf = dict(self.default_conf, **conf.get('conf', {}))
        # docker run options to be run on startup
        docker_opts = []

        version = conf.get('version', self.default_version)

        kafka = cluster.find_app(KafkaBrokerApp)
        if kafka:
            app_conf['kafkastore_bootstrap_servers'] =\
                kafka.conf.get('docker_advertised_listeners')
        else:
            raise Exception('KafkaBrokerApp required')

        # Find available port on the host
        port = trivup.TcpPortAllocator(cluster).next(
            self, conf.get('port_base', None))

        self.expose_port(docker_opts, 8081, port)

        protocol = 'http'
        ssl = cluster.find_app(SslApp)
        if ssl is not None:
            # Mount SslApp dir, reuse broker trusts/certs
            self.add_mount(docker_opts, "/ssl", ssl.root_path())

            protocol = "https"
            app_conf['listeners'] = "https://0.0.0.0:8081"
            app_conf['inter_instance_protocol'] = 'https'

            app_conf['ssl_keystore_location'] =\
                "/ssl/broker{}.keystore.jks".format(kafka.appid)
            app_conf['ssl_keystore_password'] =\
                ssl.conf.get('ssl_key_pass')
            app_conf['ssl_key_password'] =\
                ssl.conf.get('ssl_key_pass')

            app_conf['ssl_truststore_location'] =\
                "/ssl/broker{}.truststore.jks".format(kafka.appid)
            app_conf['ssl_truststore_password'] =\
                ssl.conf.get('ssl_key_pass')

        # Capture app_conf state for use on startup
        def configure():
            return ["SCHEMA_REGISTRY_{}={}".format(key.upper(), value)
                    for key, value in app_conf.items()]

        # Handle all docker specific after setting the context
        super(SchemaRegistryApp, self).__init__(cluster,
                                                sr_image,
                                                version,
                                                configure)

        # if ssl is not None:
        #     self.docker_args.append("--mount type=bind,source={},"
        #                             "destination=/ssl".format(
        #                                 ssl.root_path()))


        # self.docker_args.append('-p {}:8081'.format(port))

        self.docker_args = docker_opts
        # This is the listener address outside the docker container,
        # using port-forwarding
        self.conf['url'] = '{}://localhost:{}'.format(protocol, port)

    def operational(self):
        self.dbg('Checking if %s is operational' % self.get('url'))
        try:
            r = requests.head(self.get('url'), timeout=1.0, verify=False)
            if r.status_code >= 200 and r.status_code < 300:
                return True
            raise Exception('status_code %d' % r.status_code)
        except Exception as e:
            self.dbg('%s check failed: %s' % (self.get('url'), e))
            return False
