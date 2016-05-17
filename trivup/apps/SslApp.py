from trivup import trivup

import os
import time
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
        
        Honoured \p conf properties:
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
        self.ca_key, self.ca_cert, self.ca_srl = self.create_ca_cert(self.__class__.__name__)


    def exec_cmd (self, cmd):
        """ Run command with args, raise exception on failure. """
        r = self.execute(cmd, stdout_fd=os.devnull).wait()
        if r != 0:
            raise Exception('%s exited with status code %d' % (cmd, r))

    def mksubj (self, cn):
        """ Generate a -subj argument string """
        d = copy(self.conf)
        d['ssl_CN'] = cn
        return "/C=%(ssl_C)s/ST=%(ssl_ST)s/L=%(ssl_L)s/O=%(ssl_O)s/CN=%(ssl_CN)s" % d

    def create_ca_cert (self, cn):
        """
        Create CA cert
        @returns (key_file, cert_file, srl_file)
        """
        key = self.mkpath('ca_%s.key' % cn)
        cert = self.mkpath('ca_%s.cert' % cn)
        srl = self.mkpath('ca_%s.srl' % cn)

        self.exec_cmd('openssl req -new -x509 -keyout "%s" -out "%s" -days 10000 -passin "pass:%s" -passout "pass:%s" -subj "%s"' % 
                      (key, cert,
                       self.conf.get('ssl_key_pass'),
                       self.conf.get('ssl_key_pass'),
                       self.mksubj(cn)))
        return (key, cert, srl)


    def create_keystore (self, cn):
        """
        Create signed Java keystore for \p cn
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
        self.exec_cmd('keytool -storepass "%s" -keypass "%s" -keystore "%s" -alias localhost -validity 10000 -genkey <<EOF\n%s\nEOF' % \
                      (self.conf.get('ssl_key_pass'),
                       self.conf.get('ssl_key_pass'),
                       keystore, inblob))

        self.dbg('Adding truststore for %s: %s' % (cn, truststore))
        self.exec_cmd('keytool -storepass "%s" -keypass "%s" -keystore "%s" -alias CARoot -import -file "%s" <<EOF\nyes\nEOF' % \
                      (self.conf.get('ssl_key_pass'),
                       self.conf.get('ssl_key_pass'),
                       truststore, self.ca_cert))

        self.dbg('Export certificate for %s: %s' % (cn, cert))
        self.exec_cmd('keytool -storepass "%s" -keypass "%s" -keystore "%s" -alias localhost -certreq -file "%s"' % \
                      (self.conf.get('ssl_key_pass'),
                       self.conf.get('ssl_key_pass'),
                       keystore, cert))

        self.dbg('Sign certificate for %s' % cn)
        self.exec_cmd('openssl x509 -req -CA "%s" -CAkey "%s" -in "%s" -out "%s" -days 10000 -CAcreateserial -passin "pass:%s"' % \
                      (self.ca_cert, self.ca_key,
                       cert, signedcert,
                       self.conf.get('ssl_key_pass')))


        self.dbg('Import CA for %s' % cn)
        self.exec_cmd('keytool -storepass "%s" -keypass "%s" -keystore "%s" -alias CARoot -import -file "%s" <<EOF\nyes\nEOF' % \
                      (self.conf.get('ssl_key_pass'),
                       self.conf.get('ssl_key_pass'),
                       keystore, self.ca_cert))
        
        self.dbg('Import signed CA for %s' % cn)
        self.exec_cmd('keytool -storepass "%s" -keypass "%s" -keystore "%s" -alias localhost -import -file "%s"' % \
                      (self.conf.get('ssl_key_pass'),
                       self.conf.get('ssl_key_pass'),
                       keystore, signedcert))

        return (keystore, truststore, cert, signedcert)

    

    def create_key (self, cn):
        """
        Create standard PEM keys for \p cn.
        This is typically for clients.
        @returns (key, req, pem)
        """
        key = self.mkpath('%s.key' % cn)
        req = self.mkpath('%s.req' % cn)
        pem = self.mkpath('%s.pem' % cn)

        self.dbg('Generating key for %s: %s' % (cn, key))
        self.exec_cmd('openssl genrsa -des3 -passout "pass:%s" -out "%s" 1024' % \
                      (self.conf.get('ssl_key_pass'), key))

        self.dbg('Generating request for %s: %s' % (cn, req))
        self.exec_cmd('openssl req -passin "pass:%s" -passout "pass:%s" -key "%s" -new -out "%s" -subj "%s"' % \
                      (self.conf.get('ssl_key_pass'),
                       self.conf.get('ssl_key_pass'),
                       key, req, self.mksubj(cn)))

        self.dbg('Signing key for %s' % (cn))
        self.exec_cmd('openssl x509 -req -passin "pass:%s" -in "%s" -CA "%s" -CAkey "%s" -CAserial "%s" -out "%s"' %\
                      (self.conf.get('ssl_key_pass'),
                       req, self.ca_cert, self.ca_key, self.ca_srl, pem))

        return (key, req, pem)

                       
    def operational (self):
        return True

    def deploy (self):
        return

    def start_cmd (self):
        return None

