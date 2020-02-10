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
from trivup.apps.KerberosKdcApp import KerberosKdcApp
from trivup.apps.SslApp import SslApp

from string import Template
import contextlib
import os
import socket
import time


class KafkaBrokerApp (trivup.App):
    """ Kafka broker app
        Depends on ZookeeperApp """
    def __init__(self, cluster, conf=None, on=None):
        """
        @param cluster     Current cluster
        @param conf        Configuration dict, see below.
        @param on          Node name to run on

        Supported conf keys:
           * version - Kafka version to use, will build 'trunk' from
                       kafka_path, otherwise the version is taken to be a
                       formal release which will be downloaded and deployed.
           * listeners - CSV list of listener types:
                         PLAINTEXT,SSL,SASL,SASL_SSL
           * listener_host - alternative listener host instead of
                             node name (e.g., '*')
           * advertised_hostname - hostname to use for advertised.listeners
                                   (defaults to 'on' node)
           * sasl_mechanisms - CSV list of SASL mechanisms to enable:
                               GSSAPI,PLAIN,SCRAM-SHA-n,OAUTHBEARER
                               SASL listeners will be added automatically.
                               KerberosKdcApp is required for GSSAPI.
           * sasl_users - CSV list of SASL PLAIN/SCRAM of user=pass for
                          authenticating clients
           * ssl_client_auth - ssl.client.auth broker property (def: required)
           * num_partitions - Topic auto-create partition count (3)
           * replication_Factor - Topic auto-create replication factor (1)
           * port_base - Low TCP port base to start allocating from (random)
           * kafka_path - Path to Kafka build tree (for trunk usage)
           * fdlimit - RLIMIT_NOFILE (or "max") (default: max)
           * conf - arbitary server.properties config as a list of strings.
           * realm - Kerberos realm to use when sasl_mechanisms contains GSSAPI
        """
        super(KafkaBrokerApp, self).__init__(cluster, conf=conf, on=on)

        self.zk = cluster.find_app('ZookeeperApp')
        if self.zk is None:
            raise Exception('ZookeeperApp required')

        # Kafka repo uses SVN nomenclature
        if self.conf['version'] == 'master':
            self.conf['version'] = 'trunk'

        if 'fdlimit' not in self.conf:
            self.conf['fdlimit'] = 'max'

        listener_host = self.conf.get('listener_host',
                                      self.conf.get('nodename'))
        self.conf['listener_host'] = listener_host

        # Kafka Configuration properties
        self.conf['log_dirs'] = self.create_dir('logs')
        if 'num_partitions' not in self.conf:
            self.conf['num_partitions'] = 3
        self.conf['zk_connect'] = self.zk.get('address', None)
        if 'replication_factor' not in self.conf:
            self.conf['replication_factor'] = 1

        # Kafka paths
        if self.conf.get('kafka_path', None) is not None:
            self.conf['destdir'] = self.conf['kafka_path']
            self.conf['bindir'] = os.path.join(self.conf['destdir'], 'bin')
            start_sh = os.path.join(self.conf['bindir'],
                                    'kafka-server-start.sh')
            kafka_configs_sh = os.path.join(self.conf['bindir'],
                                            'kafka-configs.sh')
        else:
            start_sh = 'kafka-server-start.sh'
            kafka_configs_sh = 'kafka-configs.sh'

        # Arbitrary (non-template) configuration statements
        conf_blob = self.conf.get('conf', list())
        jaas_blob = list()

        #
        # Configure listeners, SSL and SASL
        #
        listeners = self.conf.get('listeners', 'PLAINTEXT').split(',')

        # SASL support
        sasl_mechs = [x for x in self.conf.get('sasl_mechanisms', '').
                      replace(' ', '').split(',') if len(x) > 0]
        if len(sasl_mechs) > 0:
            listeners.append('SASL_PLAINTEXT')

        # SSL support
        ssl = cluster.find_app(SslApp)
        if ssl is not None:
            # Add SSL listeners
            listeners.append('SSL')
            if len(sasl_mechs) > 0:
                listeners.append('SASL_SSL')

        listener_map = 'listener.security.protocol.map=' + \
            'PLAINTEXT:PLAINTEXT,SSL:SSL,SASL_PLAINTEXT:' + \
            'SASL_PLAINTEXT,SASL_SSL:SASL_SSL'

        can_docker = self.conf['version'] == 'trunk' or \
            int(self.conf['version'].split('.')[0]) > 0
        if can_docker:
            # Map DOCKER listener to PLAINTEXT security protocol
            listener_map += ',DOCKER:PLAINTEXT'

        conf_blob.append(listener_map)

        # Create listeners
        ports = [(x, trivup.TcpPortAllocator(self.cluster).next(
            self, self.conf.get('port_base', None)))
                 for x in sorted(set(listeners))]
        self.conf['port'] = ports[0][1]  # "Default" port

        if can_docker:
            # Add docker listener to allow services (e.g, SchemaRegistry) in
            # docker-containers to reach the on-host Kafka.
            docker_port = trivup.TcpPortAllocator(self.cluster).next(self)
            docker_host = '%s:%d' % (cluster.get_docker_host(), docker_port)

        self.conf['address'] = '%s:%d' % (listener_host, self.conf['port'])
        listeners = ['%s://%s:%d' % (x[0], "0.0.0.0", x[1]) for x in ports]
        if can_docker:
            listeners.append('%s://%s:%d' % ('DOCKER', "0.0.0.0", docker_port))
        self.conf['listeners'] = ','.join(listeners)
        if 'advertised_hostname' not in self.conf:
            self.conf['advertised_hostname'] = self.conf['nodename']
        advertised_listeners = ['%s://%s:%d' %
                                (x[0], self.conf['advertised_hostname'], x[1])
                                for x in ports]
        if can_docker:
            # Expose service to docker containers as well.
            advertised_listeners.append('DOCKER://%s' % docker_host)
            self.conf['docker_advertised_listeners'] = 'PLAINTEXT://%s' % \
                docker_host
        self.conf['advertised.listeners'] = ','.join(advertised_listeners)
        self.conf['advertised_listeners'] = self.conf['advertised.listeners']
        self.conf['auto_create_topics'] = self.conf.get('auto_create_topics',
                                                        'true')
        self.dbg('Listeners: %s' % self.conf['listeners'])
        self.dbg('Advertised Listeners: %s' %
                 self.conf['advertised.listeners'])

        if len(sasl_mechs) > 0:
            self.dbg('SASL mechanisms: %s' % sasl_mechs)
            jaas_blob.append('KafkaServer {')

            conf_blob.append('sasl.enabled.mechanisms=%s' %
                             ','.join(sasl_mechs))
            # Handle PLAIN and SCRAM-.. the same way
            for mech in sasl_mechs:
                if mech.find('SCRAM') != -1:
                    plugin = 'scram.Scram'
                elif mech == 'PLAIN':
                    plugin = 'plain.Plain'
                else:
                    continue

                sasl_users = self.conf.get('sasl_users', '')
                if len(sasl_users) == 0:
                    self.log(('WARNING: No sasl_users configured for %s, '
                              'expected CSV of user=pass,..') % plugin)
                else:
                    jaas_blob.append(('org.apache.kafka.common.security.'
                                      '%sLoginModule required debug=true') %
                                     plugin)
                    for up in sasl_users.split(','):
                        u, p = up.split('=')
                        if plugin == 'plain.Plain':
                            jaas_blob.append('  user_%s="%s"' % (u, p))
                        elif plugin == 'scram.Scram':
                            jaas_blob.append('  username="%s" password="%s"' %
                                             (u, p))
                            # SCRAM users are set up in ZK
                            # with kafka-configs.sh
                            self.post_start_cmds.append((
                                '%s --zookeeper %s --alter --add-config '
                                '\'%s=[iterations=4096,password=%s]\' '
                                '--entity-type users --entity-name \'%s\'') %
                                                        (kafka_configs_sh,
                                                         self.conf['zk_connect'],  # noqa: E501
                                                         mech, p, u))

                    jaas_blob[-1] += ';'

            if 'GSSAPI' in sasl_mechs:
                conf_blob.append('sasl.kerberos.service.name=%s' % 'kafka')
                realm = self.conf.get('realm', None)
                if realm is None:
                    kdc = self.cluster.find_app(KerberosKdcApp)
                    self.conf['realm'] = kdc.conf.get('realm', None)
                else:
                    kdc = self.cluster.find_app(KerberosKdcApp,
                                                ('realm', realm))
                    # If a realm was specified it is most likely because
                    # we're operating in a cross-realm scenario.
                    # Add a principal mapping client principals without
                    # hostname ("admin" rather than "admin/localhost")
                    # to a local user.
                    # This is not compatible with "admin/localhost" principals.
                    conf_blob.append('authorizer.class.name=kafka.security.auth.SimpleAclAuthorizer')  # noqa: E501
                    conf_blob.append('allow.everyone.if.no.acl.found=true')
                    conf_blob.append('sasl.kerberos.principal.to.local.rules=RULE:[1:admin](.*)s/^.*/admin/')  # noqa: E501

                assert kdc is not None, \
                    "No KerberosKdcApp found (realm={})".format(realm)
                self.env_add('KRB5_CONFIG', kdc.conf['krb5_conf'])
                self.env_add('KAFKA_OPTS', '-Djava.security.krb5.conf=%s' %
                             kdc.conf['krb5_conf'])
                self.env_add('KAFKA_OPTS', '-Dsun.security.krb5.debug=true')
                self.kerberos_principal, self.kerberos_keytab = kdc.add_principal('kafka', self.conf['advertised_hostname'])  # noqa: E501
                jaas_blob.append('com.sun.security.auth.module.Krb5LoginModule required')  # noqa: E501
                jaas_blob.append('useKeyTab=true storeKey=true doNotPrompt=true')  # noqa: E501
                jaas_blob.append('keyTab="%s"' % self.kerberos_keytab)
                jaas_blob.append('debug=true')
                jaas_blob.append('principal="%s";' % self.kerberos_principal)

            if 'OAUTHBEARER' in sasl_mechs:
                # Use the unsecure JSON web token.
                # Client should be configured with
                # 'sasl.oauthbearer.config=scope=requiredScope principal=admin'
                # Change requiredScope to something else to trigger auth error.
                conf_blob.append('super.users=User:admin')
                conf_blob.append('allow.everyone.if.no.acl.found=true')
                conf_blob.append('authorizer.class.name=kafka.security.auth.SimpleAclAuthorizer')  # noqa: E501
                jaas_blob.append('org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required')  # noqa: E501
                jaas_blob.append('  unsecuredLoginLifetimeSeconds="3600"')
                jaas_blob.append('  unsecuredLoginStringClaim_sub="admin"')
                jaas_blob.append('  unsecuredValidatorRequiredScope="requiredScope"')  # noqa: E501
                jaas_blob.append(';')

            jaas_blob.append('};\n')
            self.conf['jaas_file'] = self.create_file('jaas_broker.conf',
                                                      data='\n'.
                                                      join(jaas_blob))
            self.env_add('KAFKA_OPTS',
                         '-Djava.security.auth.login.config=%s' %
                         self.conf['jaas_file'])
            if self.cluster.debug:
                self.env_add('KAFKA_OPTS', '-Djava.security.debug=all')

        # SSL config and keys (et.al.)
        if ssl is not None:
            keystore, truststore, _, _ = ssl.create_keystore('broker%s' %
                                                             self.appid)
            conf_blob.append('ssl.protocol=TLS')
            conf_blob.append('ssl.enabled.protocols=TLSv1.2,TLSv1.1,TLSv1')
            conf_blob.append('ssl.keystore.type = JKS')
            conf_blob.append('ssl.keystore.location = %s' % keystore)
            conf_blob.append('ssl.keystore.password = %s ' %
                             ssl.conf.get('ssl_key_pass'))
            conf_blob.append('ssl.key.password = %s' %
                             ssl.conf.get('ssl_key_pass'))
            conf_blob.append('ssl.truststore.type = JKS')
            conf_blob.append('ssl.truststore.location = %s' % truststore)
            conf_blob.append('ssl.truststore.password = %s' %
                             ssl.conf.get('ssl_key_pass'))
            conf_blob.append('ssl.client.auth = %s' %
                             self.conf.get('ssl_client_auth', 'required'))

        # Enable JMX
        jmx_port = trivup.TcpPortAllocator(self.cluster).next(self)
        # FIXME: JmxTool does not work when JMX is bound to listener_host,
        #        so we unfortunately can't bind to listener_host here.
        self.conf['jmx_port'] = jmx_port
        self.env_add('JMX_PORT', str(jmx_port))

        # Generate config file
        self.conf['conf_file'] = self.create_file_from_template('server.properties',  # noqa: E501
                                                                self.conf,
                                                                append_data=Template('\n'.join(conf_blob)).substitute(self.conf))  # noqa: E501

        # Generate LOG4J file (if app debug is enabled)
        if self.debug:
            self.conf['log4j_file'] = self.create_file_from_template('log4j.properties', self.conf, subst=False)  # noqa: E501
            self.env_add('KAFKA_LOG4J_OPTS',
                         '-Dlog4j.configuration=file:%s' %
                         self.conf['log4j_file'])

        self.env_add('LOG_DIR', self.mkpath('debug'))

        # Runs in foreground, stopped by Ctrl-C
        # This is the default for no-deploy use:
        # will be overwritten by deploy() if enabled.

        self.conf['start_cmd'] = '%s %s' % (start_sh, self.conf['conf_file'])
        self.conf['stop_cmd'] = None  # Ctrl-C

    def operational(self):
        self.dbg('Checking if operational')
        addr, port = self.get('address').split(':')
        with contextlib.closing(socket.socket(socket.AF_INET,
                                              socket.SOCK_STREAM)) as s:
            return s.connect_ex((addr, int(port))) == 0

    def deploy(self):
        destdir = os.path.join(self.cluster.mkpath(self.__class__.__name__),
                               'kafka', self.get('version'))
        self.dbg('Deploy %s version %s on %s to %s' %
                 (self.name, self.get('version'), self.node.name, destdir))
        deploy_exec = self.resource_path('deploy.sh')
        if not os.path.exists(deploy_exec):
            raise NotImplementedError('Kafka deploy.sh script missing in %s' %
                                      deploy_exec)
        t_start = time.time()
        cmd = '%s %s "%s" "%s"' % \
              (deploy_exec, self.get('version'),
               self.get('kafka_path', destdir), destdir)
        r = os.system(cmd)
        if r != 0:
            raise Exception('Deploy "%s" returned exit code %d' % (cmd, r))
        self.dbg('Deployed version %s in %ds' %
                 (self.get('version'), time.time() - t_start))

        self.conf['destdir'] = destdir
        self.conf['bindir'] = os.path.join(self.conf['destdir'], 'bin')
        # Override start command with updated path.
        self.conf['start_cmd'] = '%s/bin/kafka-server-start.sh %s' % \
                                 (destdir, self.conf['conf_file'])
        self.dbg('Updated start_cmd to %s' % self.conf['start_cmd'])
        # Add kafka-dir/bin to PATH so that the bundled tools are
        # easily called.
        self.env_add('PATH', os.environ.get('PATH') + ':' +
                     os.path.join(destdir, 'bin'), append=False)
