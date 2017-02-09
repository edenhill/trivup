from trivup import trivup
import os
import time


class KerberosKdcApp (trivup.App):
    """ Kerberos KDC app
        kdc must be installed on the target node.
    """
    def __init__(self, cluster, realm, conf=None, on=None):
        """
        @param cluster     Current cluster
        @param realm       Realm name
        @param conf        Configuration dict, ignored.
        @param on          Node name to run on
        """
        super(KerberosKdcApp, self).__init__(cluster, conf=conf, on=on)

        self.conf['port'] = trivup.TcpPortAllocator(self.cluster).next(self)
        self.conf['realm'] = realm
        self.conf['address'] = '%(nodename)s:%(port)d' % self.conf
        self.conf['dbpath'] = self.mkpath('database')
        self.conf['admin_keytab'] = self.mkpath('admin_keytab')
        self.conf['stash_file'] = self.mkpath('stash_file')

        # Generate config files
        self.conf['krb5_conf'] = self.create_file_from_template('krb5.conf',
                                                                self.conf)
        self.env_add('KRB5_CONFIG', self.conf['krb5_conf'])
        self.conf['kdc_conf'] = self.create_file_from_template('kdc.conf',
                                                               self.conf)
        self.env_add('KRB5_KDC_PROFILE', self.conf['kdc_conf'])

        # Create database and stash file
        r = self.execute('kdb5_util -P "" -r %(realm)s -d "%(dbpath)s" -sf "%(stash_file)s" create -s' % self.conf).wait()
        if r != 0:
            raise Exception('Failed to create kdb5 database')

        self.conf['start_cmd'] = '/usr/sbin/krb5kdc -n'
        self.conf['stop_cmd'] = None # Ctrl-C

    def operational (self):
        self.dbg('Checking if operational: FIXME')
        return True
        #return os.system('(echo anything | nc %s) 2>/dev/null' %
        #' '.join(self.get('address').split(':'))) == 0


    def deploy (self):
        """ Requires krb5kdc to be installed through other means, e.g.:
             sudo apt-get install krb5-kdc krb5-admin-server """
        pass

    def add_principal (self, service, host):
        """
        @brief Add principal to server and generate keytab
        @returns (principal string, keytab path)
        """
        # Add principal
        principal = '%s/%s@%s' % (service, host, self.conf['realm'])
        r = self.execute('kadmin.local -d "%s" -q "addprinc -randkey %s"' % \
                         (self.conf.get('dbpath'), principal)).wait()
        if r != 0:
            raise Exception('ktadmin addprinc failed')

        # Generate keytab
        keytabdir = self.create_dir(os.path.join('keytabs', service))
        keytab = self.mkpath(os.path.join(keytabdir, host))
        r = self.execute('kadmin.local -d "%s" -q "ktadd -k "%s" %s"' % \
                         (self.conf.get('dbpath'), keytab, principal)).wait()
        if r != 0:
            raise Exception('ktadmin ktadd failed')

        # Return keytab path
        return (principal, keytab)

