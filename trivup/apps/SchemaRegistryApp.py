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

    default_version = '5.3.0'

    default_app_conf = {
        'debug': True,
        'host.name': 'localhost',
        'listeners': 'http://0.0.0.0:8081'
    }

    @staticmethod
    def _conf_to_env(app_conf):
        return ["SCHEMA_REGISTRY_{}={}"
                .format(key.replace(".", "_").upper(), value)
                for key, value in app_conf.items()]

    def __init__(self, cluster, conf=None):
        """
        @param cluster     Current cluster
        @param conf        Configuration dict, see below.
        @param on          Node name to run on

        Supported conf keys:
           * version - Confluent Platform version to use.
           * enable_auth - Enable Http basic auth
           * port_base - Low TCP port base to start allocating from (random)
           * conf - Dictionary of Confluent Schema Registry configs
        """
        print(conf.get('enable_auth'))
        # Handle all docker specific after setting the context
        super(SchemaRegistryApp, self).__init__(cluster,
                                                sr_image,
                                                conf.get('version',
                                                         self.default_version),
                                                conf)

        protocol = 'http'
        # Find available port on the host and expose it
        port = trivup.TcpPortAllocator(cluster).next(
            self, self.conf.get('port_base', None))
        self.expose_port(self.docker_args, 8081, port)

        app_conf = dict(self.default_app_conf, **conf.get('conf', {}))

        kafka = cluster.find_app(KafkaBrokerApp)
        if kafka:
            app_conf['kafkastore.bootstrap.servers'] =\
                kafka.conf.get('docker_advertised_listeners')
        else:
            raise Exception('KafkaBrokerApp required')

        ssl = cluster.find_app(SslApp)
        if ssl is not None:
            # Mount SslApp dir, reuse broker trusts/certs
            self.add_mount(self.docker_args, "/opt/ssl", ssl.root_path())
            protocol = 'https'
            app_conf.update({
                'listeners': 'https://0.0.0.0:8081',
                'inter_instance_protocol': 'https',
                'ssl_keystore_location':
                    '/opt/ssl/broker{}.keystore.jks'.format(kafka.appid),
                'ssl_keystore_password': ssl.conf.get('ssl_key_pass'),
                'ssl_key_password': ssl.conf.get('ssl_key_pass'),
                'ssl_truststore_location':
                    '/opt/ssl/broker{}.truststore.jks'.format(kafka.appid),
                'ssl_truststore_password':  ssl.conf.get('ssl_key_pass')
                })

        if bool(self.conf.get('enable_auth', False)):
            self.conf['userinfo'] = 'ckp_tester:test_secret'
            app_conf.update({'authentication.method': 'BASIC',
                             'authentication.realm': 'SchemaRegistry',
                             'authentication.roles': 'Testers'})
            app_conf['OPTS'] =\
                '-Djava.security.auth.login.config=/opt/app/'\
                'schema-registry.jaas'

        if bool(self.conf.get('enable_auth', False)):
            self.conf['userinfo'] = 'ckp_tester:test_secret'
            self.add_mount(self.docker_args, "/opt/app", self.root_path())
            self.create_file_from_template('login.properties')
            self.create_file_from_template('schema-registry.jaas', subst=False)

        self.conf['url'] = '{}://{}@localhost:{}'.format(
                protocol,
                self.conf.get('userinfo', ''),
                port)

        # format configs for use by docker
        self.app_args = self._conf_to_env(app_conf)

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
