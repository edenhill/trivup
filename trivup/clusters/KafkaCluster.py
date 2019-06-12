#!/usr/bin/env python
#
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


# conf dict structure with defaults:
# commented-out fields are not defaults but show what is available.
default_conf = {
    'version': '2.2.0',  # Apache Kafka version
    'cp_version': '5.2.1',  # Confluent Platform version (for SchemaRegistry)
    'broker_cnt': 3,
    'sasl.mechanism': '',  # or GSSAPI, PLAIN, SCRAM-.., OAUTHBEARER
    #  GSSAPI/Kerberos
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


class KafkaCluster(object):
    def __init__(self, conf=None):
        super(KafkaCluster, self).__init__()

        self.conf = deepcopy(default_conf)
        if conf is not None:
            self.conf.update(conf)

        self.version = self.conf.get('version')

        # Create trivup Cluster
        self.cluster = Cluster(
            self.__class__.__name__,
            os.environ.get('TRIVUP_ROOT', 'tmp-%s' % self.__class__.__name__),
            debug=bool(self.conf.get('debug', False)))

        self.client_conf = dict()
        self.env = dict()

        self.sasl_mechanism = self.conf.get('sasl.mechanism')

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
        self.cluster.start()

        # Set up additional convenience envs
        self._setup_env()

    def _setup_env(self):
        """ Set up convenience envs """
        self.env['KAFKA_PATH'] = self.cluster.find_app(KafkaBrokerApp).\
                                 get('destdir')
        self.env['ZK_ADDRESS'] = self.zk.get('address')
        self.env['BROKERS'] = self.bootstrap_servers
        self.env['KAFKA_VERSION'] = self.version
        self.env['TRIVUP_ROOT'] = self.cluster.instance_path()

        # Add each broker pid as an env so they can be killed indivdidually.
        for b in self.cluster.find_apps(KafkaBrokerApp, 'started'):
            self.env['BROKER_PID_%d' % b.appid] = str(b.proc.pid)


    def _setup_client_conf(self):
        """ Set up librdkafka client configuration """
        self.client_conf['bootstrap.servers'] = self.bootstrap_servers
        self.client_conf['broker.address.family'] = 'v4'
        if self.security_protocol != 'PLAINTEXT':
            self.client_conf['security.protocol'] = self.security_protocol

        broker_version = self.conf.get('version')
        brver = broker_version.split('.')
        if brver[0] == 0 and brver[1] < 10:
            self.client_conf['broker.version.fallback'] = broker_version
            self.client_conf['api.version.request'] = 'false'

        # Client SASL configuration
        if self.sasl_mechanism:
            self.client_conf['sasl.mechanism'] = self.sasl_mechanism

            if self.sasl_mechanism == 'PLAIN' or \
               self.sasl_mechanism.find('SCRAM') != -1:
                # Use first user as SASL user/pass
                for up in self.conf.get('sasl_users', '').split(','):
                    u, p = up.split('=')
                    self.client_conf['sasl.username'] = u
                    self.client_conf['sasl.username'] = p
                    break

            elif self.sasl_mechanism == 'OAUTHBEARER':
                self.client_conf['enable.sasl.oauthbearer.unsecure.jwt'] = True
                self.client.conf['sasl.oauthbearer.config'] = \
                    'scope=requiredScope principal=admin'

        # Client SSL configuration
        if self.ssl is not None:
            key = self.ssl.create_cert('client')
            self.client_conf['ssl.ca.location'] = self.ssl.ca['pem']
            self.client_conf['ssl.certificate.location'] = key['pub']['pem']
            self.client_conf['ssl.key.location'] = key['priv']['pem']
            self.client_conf['ssl.key.password'] = key['password']

            # Add envs pointing out locations of the generated certs
            for k, v in self.ssl.ca.iteritems():
                self.env['SSL_ca_{}'.format(k)] = v

            # Set envs for all generated keys so tests can find them.
            for k, v in key.iteritems():
                if type(v) is dict:
                    for k2, v2 in v.iteritems():
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

        self.client_conf['sasl.kerberos.keytab'] = keytab
        self.client_conf['sasl.kerberos.principal'] = principal.split('@')[0]
        # Refresh ticket 60s before renew timeout.
        self.client_conf['sasl.kerberos.min.time.before.relogin'] = \
            max(1, int(self.conf.get('krb_renew_lifetime')) - 60) * 1000

    def stop(self, cleanup=True, keeptypes=['log'], force=False):
        """ Stop cluster and clean up """
        self.cluster.stop(force=True)
        if cleanup:
            self.cluster.cleanup(keeptypes)

    def wait_operational(self, timeout=60):
        """ Wait for cluster to go operational """
        if not self.cluster.wait_operational(timeout):
            self.stop(force=True)
            raise Exception(
                "Cluster {} did not go operational, see logs in {}/{}".format(
                    self.cluster.name, self.cluster.root_path,
                    self.cluster.instance))

    def interactive(self):
        """ Execute an interactive shell that has all the
            environment variables set. """

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

        # Prefix the standard prompt with cluster info.
        pfx = '[TRIVUP:{}@{}] '.format(self.cluster.name, self.version)
        cmd = 'bash --rcfile <(cat ~/.bashrc; echo \'PS1="{}$PS1"\') -i'.\
                                                          format(pfx)

        print("# - You're now in an interactive sub-shell, " +
              "type 'exit' to exit back to your shell and stop the cluster.\n" +
              "# - Connect to cluster with bootstrap.servers {}".format(
                  self.bootstrap_servers))
        retcode = subprocess.call(cmd, env=env, shell=True,
                                  executable='/bin/bash')
        if retcode != 0:
            print("# - Shell exited with returncode {}: {}".format(
                retcode, cmd))

    def write_client_conf(self, path, additional_blob=None):
        """ Write client configuration (librdkafka) to \p path """
        with  open(path, "w") as f:
            for k, v in self.client_conf.iteritems():
                f.write(('%s=%s\n' % (k, v)).encode('ascii'))
            if additional_blob is not None:
                f.write(('#\n# Additional configuration:').encode('ascii'))
                f.write(additional_blob.encode('ascii'))


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Trivup KafkaCluster')
    parser.add_argument('--debug', action='store_true', dest='debug',
                        default=False, help='Enable trivup debugging')
    parser.add_argument('--sasl', dest='sasl', type=str,
                        default=default_conf['sasl.mechanism'],
                        help='SASL mechanism (PLAIN, SCRAM-SHA-nnn, GSSAPI, '+
                        'OAUTHBEARER)')
    parser.add_argument('--ssl', dest='ssl', action='store_true',
                        default=default_conf['with_ssl'],
                        help='Enable SSL')
    parser.add_argument('--sr', dest='sr', action='store_true',
                        default=default_conf['with_sr'],
                        help='Enable SchemaRegistry')
    parser.add_argument('--brokers', dest='broker_cnt', type=int,
                        default=default_conf['broker_cnt'],
                        help='Number of Kafka brokers')
    parser.add_argument('--version', dest='version', type=str,
                        default=default_conf['version'],
                        help='Apache Kafka version')

    args = parser.parse_args()

    conf = {'debug': args.debug,
            'sasl.mechanism': args.sasl,
            'with_ssl': args.ssl,
            'with_sr': args.sr,
            'broker_cnt': args.broker_cnt}

    kc = KafkaCluster(conf)

    kc.interactive()

    print("# Stopping cluster in {}/{}".format(kc.cluster.root_path,
                                               kc.cluster.instance))

    kc.stop()
