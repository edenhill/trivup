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


# Provides a Kafka cluster with the following components:
#  * ZooKeeperApp (1)
#  * KafkaBrokerApp brokers (broker_cnt=3)
#  * SslApp (optional, if with_ssl=True)
#  * KerberosKdcApp (optional, sasl.mechanism=GSSAPI,
#                    cross-realm if realm_cnt=2)
#  * SchemaRegistryApp (optional, if with_sr=True)
#
# cluster.env (dict) will contain:
#      TRIVUP_ROOT
#      ZK_ADDRESS
#      BROKERS
#      BROKER_PID_<nodeid>
#      KAFKA_PATH  (path to kafka package root directory)
#      SR_URL      (if with_sr)
#      SSL_(ca|pub|priv)_.. (if with_ssl, paths to certs and keys)
#      SSL_password         (if with_ssl, key password)
#      KRB5CCNAME, KRB5_COFNIG, KRB5_KDC_PROFILE (if GSSAPI enabled)
#
# See conf dict structure below.

from trivup.trivup import Cluster, TcpPortAllocator
from trivup.apps.ZookeeperApp import ZookeeperApp
from trivup.apps.KafkaBrokerApp import KafkaBrokerApp
from trivup.apps.KerberosKdcApp import KerberosKdcApp
from trivup.apps.SchemaRegistryApp import SchemaRegistryApp
from trivup.apps.SslApp import SslApp

from copy import deepcopy

import os
import argparse
import subprocess


class KafkaCluster(object):
    # conf dict structure with defaults:
    # commented-out fields are not defaults but show what is available.
    default_conf = {
        'version': '2.2.0',     # Apache Kafka version
        'cp_version': '5.2.1',  # Confluent Platform version (for SR)
        'broker_cnt': 3,
        'sasl_mechanism': '',   # GSSAPI, PLAIN, SCRAM-.., ...
        'realm_cnt': 1,
        'krb_renew_lifetime': 30,
        'krb_ticket_lifetime': 120,
        # SASL PLAIN/SCRAM
        'sasl_users': 'testuser=testpass',
        # With SSL
        'with_ssl': False,
        # With SchemaRegistry
        'with_sr': False,
        # Debug trivup
        'debug': False,
        # Additional broker server.properties configuration
        # 'broker_conf': ['connections.max.idle.ms=1234', ..]
    }

    def __init__(self, **kwargs):
        """ Create and start a KafkaCluster.
            See default_conf above for parameters. """
        super(KafkaCluster, self).__init__()

        conf = kwargs
        self.conf = deepcopy(self.default_conf)
        if conf is not None:
            self.conf.update(conf)

        self.version = self.conf.get('version')

        # Create trivup Cluster
        self.cluster = Cluster(
            self.__class__.__name__,
            os.environ.get('TRIVUP_ROOT', 'tmp-%s' % self.__class__.__name__),
            debug=bool(self.conf.get('debug', False)))

        self._client_conf = dict()
        self.env = dict()

        self.sasl_mechanism = self.conf.get('sasl_mechanism')

        # Generate SSL certs if enabled
        if bool(self.conf.get('with_ssl')):
            self.ssl = SslApp(self.cluster, self.conf)
        else:
            self.ssl = None

        # Map mechanism and SSL to security protocol
        self.security_protocol = {(True, True): 'SASL_SSL',
                                  (True, False): 'SASL_PLAINTEXT',
                                  (False, True): 'SSL',
                                  (False, False): 'PLAINTEXT'}[
                                      (bool(self.sasl_mechanism),
                                       bool(self.ssl is not None))]

        # Create single ZK for the cluster (don't start yet')
        self.zk = ZookeeperApp(self.cluster)

        # Broker configuration
        broker_cnt = int(self.conf.get('broker_cnt'))
        self.broker_conf = {'replication_factor': min(3, broker_cnt),
                            'num_partitions': 4,
                            'version': self.version,
                            'sasl_mechanisms': self.sasl_mechanism,
                            'sasl_users': self.conf.get('sasl_users'),
                            'conf': self.conf.get('broker_conf', [])}

        # Start Kerberos KDCs if GSSAPI (Kerberos) is configured
        if self.sasl_mechanism == 'GSSAPI':
            self._setup_kerberos()
            self.broker_conf['realm'] = self.broker_realm

        # Create brokers (don't start yet)
        self.brokers = dict()
        for n in range(0, broker_cnt):
            broker = KafkaBrokerApp(self.cluster, self.broker_conf)
            self.brokers[broker.appid] = broker

        # Generate bootstrap servers list
        all_listeners = (','.join(self.cluster.get_all(
            'advertised_listeners', '', KafkaBrokerApp))).split(',')
        self.bootstrap_servers = ','.join(
            [x for x in all_listeners if x.startswith(self.security_protocol)])

        assert len(self.bootstrap_servers) >= broker_cnt, \
            "{} < {} expected bootstrap servers".format(
                len(self.bootstrap_servers), broker_cnt)

        # Create SchemaRegistry if enabled
        if bool(self.conf.get('with_sr', False)):
            self.sr = SchemaRegistryApp(
                self.cluster, {'version': self.conf.get('cp_version')})
            self.env['SR_URL'] = self.sr.get('url')

        # Create librdkafka client configuration
        self._setup_client_conf()

        # Deploy cluster
        self.cluster.deploy()

        # Start cluster
        self.start()

    def __del__(self):
        """ Destructor: forcibly stop the cluster """
        self.stop(force=True)

    def _setup_env(self):
        """ Set up convenience envs """
        self.env['KAFKA_PATH'] = self.cluster.find_app(KafkaBrokerApp).get('destdir')  # noqa: E501
        self.env['ZK_ADDRESS'] = self.zk.get('address')
        self.env['BROKERS'] = self.bootstrap_servers
        self.env['KAFKA_VERSION'] = self.version
        self.env['TRIVUP_ROOT'] = self.cluster.instance_path()

        # Add each broker pid as an env so they can be killed indivdidually.
        for b in self.cluster.find_apps(KafkaBrokerApp, 'started'):
            self.env['BROKER_PID_%d' % b.appid] = str(b.proc.pid)

    def _setup_client_conf(self):
        """ Set up librdkafka client configuration """
        self._client_conf['bootstrap.servers'] = self.bootstrap_servers
        self._client_conf['broker.address.family'] = 'v4'
        if self.security_protocol != 'PLAINTEXT':
            self._client_conf['security.protocol'] = self.security_protocol

        broker_version = self.conf.get('version')
        brver = broker_version.split('.')
        if brver[0] == 0 and brver[1] < 10:
            self._client_conf['broker.version.fallback'] = broker_version
            self._client_conf['api.version.request'] = 'false'

        # Client SASL configuration
        if self.sasl_mechanism:
            self._client_conf['sasl.mechanism'] = self.sasl_mechanism

            if self.sasl_mechanism == 'PLAIN' or \
               self.sasl_mechanism.find('SCRAM') != -1:
                # Use first user as SASL user/pass
                for up in self.conf.get('sasl_users', '').split(','):
                    u, p = up.split('=')
                    self._client_conf['sasl.username'] = u
                    self._client_conf['sasl.password'] = p
                    break

            elif self.sasl_mechanism == 'OAUTHBEARER':
                self._client_conf['enable.sasl.oauthbearer.unsecure.jwt'] = True  # noqa: E501
                self._client_conf['sasl.oauthbearer.config'] = \
                    'scope=requiredScope principal=admin'

        # Client SSL configuration
        if self.ssl is not None:
            key = self.ssl.create_cert('client')
            self._client_conf['ssl.ca.location'] = self.ssl.ca['pem']
            self._client_conf['ssl.certificate.location'] = key['pub']['pem']
            self._client_conf['ssl.key.location'] = key['priv']['pem']
            self._client_conf['ssl.key.password'] = key['password']

            # Add envs pointing out locations of the generated certs
            for k, v in self.ssl.ca.items():
                self.env['SSL_ca_{}'.format(k)] = v

            # Set envs for all generated keys so tests can find them.
            for k, v in key.items():
                if type(v) is dict:
                    for k2, v2 in v.items():
                        # E.g. "SSL_priv_der=path/to/librdkafka-priv.der"
                        self.env['SSL_{}_{}'.format(k, k2)] = v2
                else:
                    self.env['SSL_{}'.format(k)] = v

    def _setup_kerberos(self):
        """ Set up Kerberos KDCs """

        # Create KDCs for each realm.
        # First realm will be the default / broker realm.
        #
        realm_cnt = int(self.conf.get('realm_cnt', 1))
        # No point in having more than two realms
        assert realm_cnt > 0 and realm_cnt < 3
        realms = ['REALM{}.TRIVUP'.format(x + 1) for x in range(0, realm_cnt)]

        # Pre-Allocate ports for the KDCs so they can reference eachother
        # in the krb5.conf configuration.
        kdc_ports = {x: TcpPortAllocator(
            self.cluster).next("dummy") for x in realms}

        # Set up realm=kdc:port cross-realm mappings
        cross_realms = ",".join(["{}={}:{}".format(
            x, self.cluster.get_node().name, kdc_ports[x]) for x in realms])

        kdcs = dict()
        for realm in realms:
            kdc = KerberosKdcApp(
                self.cluster, realm,
                conf={'port': kdc_ports[realm],
                      'cross_realms': cross_realms,
                      'renew_lifetime':
                      int(self.conf.get('krb_renew_lifetime')),
                      'ticket_lifetime':
                      int(self.conf.get('krb_ticket_lifetime'))})
            # Kerberos needs to be started prior to Kafka so that principals
            # and keytabs are available at the time of Kafka config generation.
            kdc.start()
            kdcs[realm] = kdc

        self.broker_realm = realms[0]
        self.client_realm = realms[-1]
        self.broker_kdc = kdcs[self.broker_realm]
        self.client_kdc = kdcs[self.client_realm]

        # Add cross-realm TGTs
        if realm_cnt > 1:
            KerberosKdcApp.add_cross_realm_tgts(kdcs)

        # Add client envs and configuration
        self.env['KRB5CCNAME'] = self.client_kdc.mkpath('krb5cc')
        self.env['KRB5_CONFIG'] = self.client_kdc.conf['krb5_conf']
        self.env['KRB5_KDC_PROFILE'] = self.client_kdc.conf['kdc_conf']
        principal, keytab = self.client_kdc.add_principal('admin')

        self._client_conf['sasl.kerberos.keytab'] = keytab
        self._client_conf['sasl.kerberos.principal'] = principal.split('@')[0]
        # Refresh ticket 60s before renew timeout.
        self._client_conf['sasl.kerberos.min.time.before.relogin'] = \
            max(1, int(self.conf.get('krb_renew_lifetime')) - 60) * 1000

    def start(self, timeout=0):
        """ Start cluster """
        self.cluster.start()

        # Set up additional convenience envs
        self._setup_env()

        if timeout > 0:
            self.wait_operational(timeout)

    def stop(self, cleanup=True, keeptypes=['log'], force=False, timeout=0):
        """ Stop cluster and clean up """
        self.cluster.stop(force=True)
        if timeout > 0:
            self.cluster.wait_stopped(timeout)
        if cleanup:
            self.cluster.cleanup(keeptypes)

    def stopped(self):
        """ Returns True when all components of the cluster are stopped """
        return len([x for x in self.cluster.apps if
                    x.status() == 'stopped']) == len(self.cluster.apps)

    def wait_operational(self, timeout=60):
        """ Wait for cluster to go operational """
        if not self.cluster.wait_operational(timeout):
            self.stop(force=True)
            raise Exception(
                "Cluster {} did not go operational, see logs in {}/{}".format(
                    self.cluster.name, self.cluster.root_path,
                    self.cluster.instance))

    def stop_broker(self, broker_id):
        """ Stop single broker """
        broker = self.brokers.get(broker_id, None)
        if broker is None:
            raise LookupError("Unknown broker id {}".format(broker_id))
        broker.stop()

    def start_broker(self, broker_id):
        """ Start single broker """
        broker = self.brokers.get(broker_id, None)
        if broker is None:
            raise LookupError("Unknown broker id {}".format(broker_id))
        broker.start()

    def interactive(self, cmd=None):
        """ Execute an interactive shell that has all the
            environment variables set. """

        if cmd is None:
            print('# Interactive mode')

        print("# - Waiting for cluster to go operational in {}/{}".format(
            self.cluster.root_path, self.cluster.instance))
        kc.wait_operational()

        env = self.env.copy()

        # Avoids 'dumb' terminal mode
        env['TERM'] = os.environ.get('TERM', 'vt100')

        # Write librdkafka client configuration to file.
        cf_path = self.cluster.mkpath('rdkafka.conf', in_instance=True)
        self.write_client_conf(cf_path)
        env['RDKAFKA_TEST_CONF'] = cf_path
        print("# - Client configuration in {}".format(cf_path))
        print("# - Connect to cluster with bootstrap.servers {}".format(
            self.bootstrap_servers))

        # Prefix the standard prompt with cluster info.
        if cmd is None:
            pfx = '[TRIVUP:{}@{}] '.format(self.cluster.name, self.version)
            fullcmd = 'bash --rcfile <(cat ~/.bashrc; echo \'PS1="{}$PS1"\') -i'.format(pfx)  # noqa: E501
            print("# - You're now in an interactive sub-shell, type 'exit' "
                  "to stop the cluster and exit back to your shell.\n")
            retcode = subprocess.call(fullcmd, env=env, shell=True,
                                      executable='/bin/bash')

        else:
            fullcmd = cmd
            print("# - Executing: {}".format(fullcmd))
            retcode = subprocess.call(fullcmd, env=env, shell=True)

        if retcode != 0:
            print("# - Shell exited with returncode {}: {}".format(
                retcode, fullcmd))

    def client_conf(self):
        """ Get a dict copy of the client configuration """
        return deepcopy(self._client_conf)

    def write_client_conf(self, path, additional_blob=None):
        """ Write client configuration (librdkafka) to @param path """
        with open(path, "w") as f:
            for k, v in self._client_conf.items():
                f.write(str('%s=%s\n' % (k, v)))
            if additional_blob is not None:
                f.write(str('#\n# Additional configuration:'))
                f.write(str(additional_blob))


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Trivup KafkaCluster')
    parser.add_argument('--debug', action='store_true', dest='debug',
                        default=False, help='Enable trivup debugging')
    parser.add_argument('--sasl', dest='sasl', type=str,
                        default=KafkaCluster.default_conf['sasl_mechanism'],
                        help='SASL mechanism (PLAIN, SCRAM-SHA-nnn, GSSAPI, ' +
                        'OAUTHBEARER)')
    parser.add_argument('--ssl', dest='ssl', action='store_true',
                        default=KafkaCluster.default_conf['with_ssl'],
                        help='Enable SSL')
    parser.add_argument('--sr', dest='sr', action='store_true',
                        default=KafkaCluster.default_conf['with_sr'],
                        help='Enable SchemaRegistry')
    parser.add_argument('--brokers', dest='broker_cnt', type=int,
                        default=KafkaCluster.default_conf['broker_cnt'],
                        help='Number of Kafka brokers')
    parser.add_argument('--version', dest='version', type=str,
                        default=KafkaCluster.default_conf['version'],
                        help='Apache Kafka version')
    parser.add_argument('--cmd', type=str, dest='cmd', default=None,
                        help='Command to execute instead of interactive shell')

    args = parser.parse_args()

    conf = {'debug': args.debug,
            'version': args.version,
            'sasl_mechanism': args.sasl,
            'with_ssl': args.ssl,
            'with_sr': args.sr,
            'broker_cnt': args.broker_cnt}

    kc = KafkaCluster(**conf)

    kc.interactive(args.cmd)

    print("# Stopping cluster in {}/{}".format(kc.cluster.root_path,
                                               kc.cluster.instance))

    kc.stop()
