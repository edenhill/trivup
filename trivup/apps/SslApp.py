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

import os
from copy import copy


class SslApp (trivup.App):
    """ Generates SSL certificates for use by other apps.
        This is not a running app but simply provides helper methods
        to generate certificates, etc. """
    def __init__(self, cluster, conf=None, on=None):
        """
        @param cluster     Current cluster
        @param conf        Configuration dict, see below.
        @param on          Node name to run on

        Honoured @param conf properties:
         * ssl_key_pass - SSL keytab password (default: 12345678)
         * SSL_{OU,O,L,S,ST,C} - (defaults: NN)

        """
        super(SslApp, self).__init__(cluster, conf=conf, on=on)

        self.conf.setdefault('ssl_key_pass', '12345678')
        self.conf.setdefault('ssl_OU', 'OU')
        self.conf.setdefault('ssl_O', 'O')
        self.conf.setdefault('ssl_L', 'L')
        self.conf.setdefault('ssl_ST', 'ST')
        self.conf.setdefault('ssl_S', 'S')
        self.conf.setdefault('ssl_C', 'NN')
        self.conf.setdefault('ssl_user', os.getenv('USER', 'NN'))

        # Generate a CA cert
        self.ca = self.create_ca_cert(self.__class__.__name__)

    def exec_cmd(self, cmd):
        """ Run command with args, raise exception on failure. """
        r = self.execute(cmd, stdout_fd=os.devnull).wait()
        if r != 0:
            raise Exception('%s exited with status code %d' % (cmd, r))

    def mksubj(self, cn):
        """ Generate a -subj argument string """
        d = copy(self.conf)
        d['ssl_CN'] = cn
        return "/C=%(ssl_C)s/ST=%(ssl_ST)s/L=%(ssl_L)s/O=%(ssl_O)s/CN=%(ssl_CN)s" % d  # noqa: E501

    def create_ca_cert(self, cn):
        """
        Create CA cert
        @returns {'pem': .., 'der': .., 'key': .., 'srl': .., 'password': ..}
        """
        ret = {'key': self.mkpath('ca_%s.key' % cn),
               'srl': self.mkpath('ca_%s.srl' % cn),
               'pem': self.mkpath('ca_%s.pem' % cn),
               'der': self.mkpath('ca_%s.der' % cn),
               'password': self.conf.get('ssl_key_pass')}

        self.dbg('Generating CA cert for %s in %s' % (cn, ret['pem']))
        self.exec_cmd('openssl req -new -x509 -keyout "%s" -out "%s" -days 10000 -passin "pass:%s" -passout "pass:%s" -subj "%s"' %  # noqa: E501
                      (ret['key'], ret['pem'],
                       ret['password'], ret['password'],
                       self.mksubj(cn)))

        self.dbg('Convert CA PEM to DER')
        self.exec_cmd('openssl x509 -outform der -in "%s" -out "%s"' %
                      (ret['pem'], ret['der']))
        return ret

    def create_keystore(self, cn):
        """
        Create signed Java keystore for @param cn
        @returns (keystore, truststore, cert, signedcert)
        """
        keystore = self.mkpath('%s.keystore.jks' % cn)
        truststore = self.mkpath('%s.truststore.jks' % cn)
        cert = self.mkpath('%s.cert' % cn)
        signedcert = self.mkpath('%s.signedcert' % cn)

        d = copy(self.conf)
        d.update({'ssl_CN': cn})
        inblob = """%(ssl_CN)s
%(ssl_OU)s
%(ssl_O)s
%(ssl_L)s
%(ssl_S)s
%(ssl_C)s
yes""" % d

        self.dbg('Generating key for %s: %s' % (cn, keystore))
        self.exec_cmd('keytool -keyalg RSA -storepass "%s" -keypass "%s" -keystore "%s" -alias localhost -validity 10000 -genkey <<EOF\n%s\nEOF' %  # noqa: E501
                      (self.conf.get('ssl_key_pass'),
                       self.conf.get('ssl_key_pass'),
                       keystore, inblob))

        self.dbg('Adding truststore for %s: %s' % (cn, truststore))
        self.exec_cmd('keytool -storepass "%s" -keypass "%s" -keystore "%s" -alias CARoot -import -file "%s" <<EOF\nyes\nEOF' %  # noqa: E501
                      (self.conf.get('ssl_key_pass'),
                       self.conf.get('ssl_key_pass'),
                       truststore, self.ca['pem']))

        self.dbg('Export certificate for %s: %s' % (cn, cert))
        self.exec_cmd('keytool -storepass "%s" -keypass "%s" -keystore "%s" -alias localhost -certreq -file "%s"' %  # noqa: E501
                      (self.conf.get('ssl_key_pass'),
                       self.conf.get('ssl_key_pass'),
                       keystore, cert))

        self.dbg('Sign certificate for %s' % cn)
        self.exec_cmd('openssl x509 -req -CA "%s" -CAkey "%s" -in "%s" -out "%s" -days 10000 -CAcreateserial -passin "pass:%s"' %  # noqa: E501
                      (self.ca['pem'], self.ca['key'],
                       cert, signedcert,
                       self.conf.get('ssl_key_pass')))

        self.dbg('Import CA for %s' % cn)
        self.exec_cmd('keytool -storepass "%s" -keypass "%s" -keystore "%s" -alias CARoot -import -file "%s" <<EOF\nyes\nEOF' %  # noqa: E501
                      (self.conf.get('ssl_key_pass'),
                       self.conf.get('ssl_key_pass'),
                       keystore, self.ca['pem']))

        self.dbg('Import signed CA for %s' % cn)
        self.exec_cmd('keytool -storepass "%s" -keypass "%s" -keystore "%s" -alias localhost -import -file "%s"' %  # noqa: E501
                      (self.conf.get('ssl_key_pass'),
                       self.conf.get('ssl_key_pass'),
                       keystore, signedcert))

        return (keystore, truststore, cert, signedcert)

    def create_cert(self, cn):
        """
        Create certificate/keys, in multiple formats (PEM, DER, PKCS#12),
        for @param cn.
        This is typically used for clients.
        The PKCS contains private key, public key, and CA cert
        @returns {'priv': {'pem': .., 'der': ..},
                  'pub': {'pem': .., 'der': ..},
                  'pkcs': '..',
                  'req': '..',
                  'password': '..'}
        """

        password = self.conf.get('ssl_key_pass')

        ret = {'priv': {'pem': self.mkpath('%s-priv.pem' % cn),
                        'der': self.mkpath('%s-priv.der' % cn)},
               'pub': {'pem': self.mkpath('%s-pub.pem' % cn),
                       'der': self.mkpath('%s-pub.der' % cn)},
               'pkcs': self.mkpath('%s.pfx' % cn),
               'req': self.mkpath('%s.req' % cn),
               'password': password}

        self.dbg('Generating key for %s: %s' % (cn, ret['priv']['pem']))
        self.exec_cmd('openssl genrsa -des3 -passout "pass:%s" -out "%s" 1024' %  # noqa: E501
                      (password, ret['priv']['pem']))

        self.dbg('Generating request for %s: %s' % (cn, ret['req']))
        self.exec_cmd('openssl req -passin "pass:%s" -passout "pass:%s" -key "%s" -new -out "%s" -subj "%s"' %  # noqa: E501
                      (password, password,
                       ret['priv']['pem'], ret['req'], self.mksubj(cn)))

        self.dbg('Signing key for %s' % (cn))
        self.exec_cmd('openssl x509 -req -passin "pass:%s" -in "%s" -CA "%s" -CAkey "%s" -CAserial "%s" -out "%s"' %  # noqa: E501
                      (password,
                       ret['req'], self.ca['pem'], self.ca['key'],
                       self.ca['srl'], ret['pub']['pem']))

        self.dbg('Converting public-key X.509 to DER for %s' % cn)
        self.exec_cmd('openssl x509 -outform der -in "%s" -out "%s"' %  # noqa: E501
                      (ret['pub']['pem'], ret['pub']['der']))

        self.dbg('Converting private-key X.509 to DER for %s' % cn)
        self.exec_cmd('openssl rsa -outform der -passin "pass:%s" -in "%s" -out "%s"' %  # noqa: E501
                      (password, ret['priv']['pem'], ret['priv']['der']))

        self.dbg('Creating PKCS#12 for %s in %s' % (cn, ret['pkcs']))
        self.exec_cmd('openssl pkcs12 -export -out "%s" -inkey "%s" -in "%s" -CAfile "%s" -certfile "%s" -passin "pass:%s" -passout "pass:%s"' %  # noqa: E501
                      (ret['pkcs'],
                       ret['priv']['pem'],
                       ret['pub']['pem'],
                       self.ca['pem'],
                       self.ca['pem'],
                       password, password))

        return ret

    def operational(self):
        return True

    def deploy(self):
        return

    def start_cmd(self):
        return None
