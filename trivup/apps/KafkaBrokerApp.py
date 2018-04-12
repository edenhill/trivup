from trivup import trivup
from trivup.apps.KerberosKdcApp import KerberosKdcApp

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
           * version - Kafka version to use, will build 'trunk' from kafka_path,
                       otherwise the version is taken to be a formal release which
                       will be downloaded and deployed.
           * listeners - CSV list of listener types: PLAINTEXT,SSL,SASL,SASL_SSL
           * listener_host - alternative listener host instead of node name (e.g., '*')
           * advertised_hostname - hostname to use for advertised.listeners (defaults to 'on' node)
           * sasl_mechanisms - CSV list of SASL mechanisms to enable: GSSAPI,PLAIN,SCRAM-SHA-n
                               SASL listeners will be added automatically.
                               KerberosKdcApp is required for GSSAPI.
           * sasl_users - CSV list of SASL PLAIN/SCRAM of user=pass for authenticating clients
           * ssl_client_auth - ssl.client.auth broker property (def: required)
           * num_partitions - Topic auto-create partition count (3)
           * replication_Factor - Topic auto-create replication factor (1)
           * port_base - Low TCP port base to start allocating from (random)
           * kafka_path - Path to Kafka build tree (for trunk usage)
           * fdlimit - RLIMIT_NOFILE (default: system)
           * conf - arbitary server.properties config as a list of strings.
        """
        super(KafkaBrokerApp, self).__init__(cluster, conf=conf, on=on)

        self.zk = cluster.find_app('ZookeeperApp')
        if self.zk is None:
            raise Exception('ZookeeperApp required')

        # Kafka repo uses SVN nomenclature
        if self.conf['version'] == 'master':
            self.conf['version'] = 'trunk'

        if 'fdlimit' not in self.conf:
            self.conf['fdlimit'] = 50000

        listener_host = self.conf.get('listener_host', self.conf.get('nodename'))
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
            start_sh = os.path.join(self.conf['bindir'], 'kafka-server-start.sh')
            kafka_configs_sh = os.path.join(self.conf['bindir'], 'kafka-configs.sh')
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
        sasl_mechs = [x for x in self.conf.get('sasl_mechanisms', '').replace(' ', '').split(',') if len(x) > 0]
        if len(sasl_mechs) > 0:
            listeners.append('SASL_PLAINTEXT')

        # SSL support
        if getattr(cluster, 'ssl', None) is not None:
            # Add SSL listeners
            listeners.append('SSL')
            if len(sasl_mechs) > 0:
                listeners.append('SASL_SSL')

        # Create listeners
        ports = [(x, trivup.TcpPortAllocator(self.cluster).next(self, self.conf.get('port_base', None))) for x in sorted(set(listeners))]
        self.conf['port'] = ports[0][1] # "Default" port
        self.conf['address'] = '%s:%d' % (listener_host, self.conf['port'])
        self.conf['listeners'] = ','.join(['%s://%s:%d' % (x[0], listener_host, x[1]) for x in ports])
        if 'advertised_hostname' not in self.conf:
            self.conf['advertised_hostname'] = self.conf['nodename']
        self.conf['advertised.listeners'] =','.join(['%s://%s:%d' % (x[0], self.conf['advertised_hostname'], x[1]) for x in ports])
        self.conf['auto_create_topics'] = self.conf.get('auto_create_topics', 'true')
        self.dbg('Listeners: %s' % self.conf['listeners'])
        self.dbg('Advertised Listeners: %s' % self.conf['advertised.listeners'])

        if len(sasl_mechs) > 0:
            self.dbg('SASL mechanisms: %s' % sasl_mechs)
            jaas_blob.append('KafkaServer {')

            conf_blob.append('sasl.enabled.mechanisms=%s' % ','.join(sasl_mechs))
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
                    self.log('WARNING: No sasl_users configured for %s, expected CSV of user=pass,..' % plugin)
                else:
                    jaas_blob.append('org.apache.kafka.common.security.%sLoginModule required debug=true' % plugin)
                    for up in sasl_users.split(','):
                        u,p = up.split('=')
                        if plugin is 'plain.Plain':
                            jaas_blob.append('  user_%s="%s"' % (u, p))
                        elif plugin is 'scram.Scram':
                            jaas_blob.append('  username="%s" password="%s"' % (u, p))
                            # SCRAM users are set up in ZK with kafka-configs.sh
                            self.post_start_cmds.append('%s --zookeeper %s --alter --add-config \'%s=[iterations=4096,password=%s]\' --entity-type users --entity-name \'%s\'' % \
                                                        (kafka_configs_sh, self.conf['zk_connect'], mech, p, u))

                    jaas_blob[-1] += ';'

            if 'GSSAPI' in sasl_mechs:
                conf_blob.append('sasl.kerberos.service.name=%s' % 'kafka')
                kdc = self.cluster.find_app(KerberosKdcApp)
                self.env_add('KRB5_CONFIG', kdc.conf['krb5_conf'])
                self.env_add('KAFKA_OPTS', '-Djava.security.krb5.conf=%s' % kdc.conf['krb5_conf'])
                self.env_add('KAFKA_OPTS', '-Dsun.security.krb5.debug=true')
                self.kerberos_principal,self.kerberos_keytab = kdc.add_principal('kafka', self.conf['advertised_hostname'])
                jaas_blob.append('com.sun.security.auth.module.Krb5LoginModule required')
                jaas_blob.append('useKeyTab=true storeKey=true doNotPrompt=true')
                jaas_blob.append('keyTab="%s"' % self.kerberos_keytab)
                jaas_blob.append('debug=true')
                jaas_blob.append('principal="%s";' % self.kerberos_principal)

            jaas_blob.append('};\n')
            self.conf['jaas_file'] = self.create_file('jaas_broker.conf', data='\n'.join(jaas_blob))
            self.env_add('KAFKA_OPTS', '-Djava.security.auth.login.config=%s' % self.conf['jaas_file'])
            self.env_add('KAFKA_OPTS', '-Djava.security.debug=all')

        # SSL config and keys (et.al.)
        if getattr(cluster, 'ssl', None) is not None:
            ssl = cluster.ssl
            keystore,truststore,_,_ = ssl.create_keystore('broker%s' % self.appid)
            conf_blob.append('ssl.protocol=TLS')
            conf_blob.append('ssl.enabled.protocols=TLSv1.2,TLSv1.1,TLSv1')
            conf_blob.append('ssl.keystore.type = JKS')
            conf_blob.append('ssl.keystore.location = %s' % keystore)
            conf_blob.append('ssl.keystore.password = %s ' % ssl.conf.get('ssl_key_pass'))
            conf_blob.append('ssl.key.password = %s' % ssl.conf.get('ssl_key_pass'))
            conf_blob.append('ssl.truststore.type = JKS')
            conf_blob.append('ssl.truststore.location = %s' % truststore)
            conf_blob.append('ssl.truststore.password = %s' % ssl.conf.get('ssl_key_pass'))
            conf_blob.append('ssl.client.auth = %s' % self.conf.get('ssl_client_auth', 'required'))


        # Generate config file
        self.conf['conf_file'] = self.create_file_from_template('server.properties',
                                                                self.conf,
                                                                append_data='\n'.join(conf_blob))

        # Generate LOG4J file (if app debug is enabled)
        if self.debug:
            self.conf['log4j_file'] = self.create_file_from_template('log4j.properties', self.conf, subst=False)
            self.env_add('KAFKA_LOG4J_OPTS', '-Dlog4j.configuration=file:%s' % self.conf['log4j_file'])

        self.env_add('LOG_DIR', self.mkpath('debug'))

        # Runs in foreground, stopped by Ctrl-C
        # This is the default for no-deploy use: will be overwritten by deploy() if enabled.

        self.conf['start_cmd'] = '%s %s' % (start_sh, self.conf['conf_file'])
        self.conf['stop_cmd'] = None # Ctrl-C

    def operational (self):
        self.dbg('Checking if operational')
        addr, port = self.get('address').split(':')
        with contextlib.closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
            return s.connect_ex((addr, int(port))) == 0

    def deploy (self):
        destdir = os.path.join(self.cluster.mkpath(self.__class__.__name__), 'kafka', self.get('version'))
        self.dbg('Deploy %s version %s on %s to %s' %
                 (self.name, self.get('version'), self.node.name, destdir))
        deploy_exec = self.resource_path('deploy.sh')
        if not os.path.exists(deploy_exec):
            raise NotImplementedError('Kafka deploy.sh script missing in %s' %
                                      deploy_exec)
        t_start = time.time()
        cmd = '%s %s "%s" "%s"' % \
              (deploy_exec, self.get('version'), self.get('kafka_path', destdir), destdir)
        r = os.system(cmd)
        if r != 0:
            raise Exception('Deploy "%s" returned exit code %d' % (cmd, r))
        self.dbg('Deployed version %s in %ds' %
                 (self.get('version'), time.time() - t_start))

        self.conf['destdir'] = destdir
        self.conf['bindir'] = os.path.join(self.conf['destdir'], 'bin')
        # Override start command with updated path.
        self.conf['start_cmd'] = '%s/bin/kafka-server-start.sh %s' % (destdir, self.conf['conf_file'])
        self.dbg('Updated start_cmd to %s' % self.conf['start_cmd'])
        # Add kafka-dir/bin to PATH so that the bundled tools are
        # easily called.
        self.env_add('PATH', os.environ.get('PATH') + ':' + os.path.join(destdir, 'bin'), append=False)
