from trivup import trivup
import os


class KerberosKdcApp (trivup.App):
    """ Kerberos KDC app
        kdc must be installed on the target node.
    """
    def __init__(self, cluster, realm, conf=None, on=None):
        """
        @param cluster     Current cluster
        @param realm       Realm name
        @param conf        Configuration dict, optional.
           "port": port to bind to.
           "cross_realms": "realm1=kdc1:port,realm2=kdc2:port" - cross-realm.
                           The first cross realm is the $default_realm.
           "renew_lifetime": see krb5.conf docs  (default 10 min)
           "ticket_lifetime": see krb5.conf docs (default 60 min)
        @param on          Node name to run on
        """
        super(KerberosKdcApp, self).__init__(cluster, conf=conf, on=on)

        self.conf['realm'] = realm
        if self.conf.get('port', None) is None:
            self.conf['port'] = trivup.TcpPortAllocator(self.cluster).next(self)  # noqa: E501
        self.conf['address'] = '%(nodename)s:%(port)d' % self.conf
        self.conf['dbpath'] = self.mkpath('database')
        self.conf['admin_keytab'] = self.mkpath('admin_keytab')
        self.conf['stash_file'] = self.mkpath('stash_file')

        if self.conf.get('renew_lifetime', None) is None:
            self.conf['renew_lifetime'] = '12h'

        if self.conf.get('ticket_lifetime', None) is None:
            self.conf['ticket_lifetime'] = '30m'

        # Set up cross-realm trusts, if desired.
        cross_realms = self.conf.get('cross_realms', '').split(',')
        if len(cross_realms) > 0 and cross_realms[0] != '':
            cross_realms_conf = ""
            capaths_conf = ""
            for crinfo in cross_realms:
                crealm, ckdc = crinfo.split('=')
                if crealm == realm:
                    continue
                cross_realms_conf += " %s = {\n  kdc = %s\n  admin_server = %s\n }\n" % (crealm, ckdc, ckdc)  # noqa: E501
                capaths_conf += " %s = {\n  %s = .\n }\n" % (crealm, realm)
                capaths_conf += " %s = {\n  %s = .\n }\n" % (realm, crealm)

            self.conf['default_realm'] = cross_realms[0].split('=')[0]
            self.conf['cross_realms'] = cross_realms_conf
            self.conf['capaths'] = capaths_conf
        else:
            self.conf['default_realm'] = realm
            self.conf['cross_realms'] = ''
            self.conf['capaths'] = ''

        # Generate config files
        self.conf['krb5_conf'] = self.create_file_from_template('krb5.conf',
                                                                self.conf)
        self.env_add('KRB5_CONFIG', self.conf['krb5_conf'])
        self.conf['kdc_conf'] = self.create_file_from_template('kdc.conf',
                                                               self.conf)
        self.env_add('KRB5_KDC_PROFILE', self.conf['kdc_conf'])

        # Create database and stash file
        r = self.execute('kdb5_util -P "" -r %(realm)s -d "%(dbpath)s" -sf "%(stash_file)s" create -s' % self.conf).wait()  # noqa: E501
        if r != 0:
            raise Exception('Failed to create kdb5 database')

        self.conf['start_cmd'] = '/usr/sbin/krb5kdc -n'
        self.conf['stop_cmd'] = None  # Ctrl-C

    def operational(self):
        self.dbg('Checking if operational: FIXME')
        return True

    def deploy(self):
        """ Requires krb5kdc to be installed through other means, e.g.:
             sudo apt-get install krb5-kdc krb5-admin-server """
        pass

    def add_principal(self, primary, instance=None):
        """
        @brief Add principal to server and generate keytab
        @param primary The principal primary ("primary/instance@realm")
        @param instance The principal instance, optional.
        @returns (principal string, keytab path)
        """
        # Add principal
        if instance is not None:
            principal = '%s/%s@%s' % (primary, instance, self.conf['realm'])
        else:
            principal = '%s@%s' % (primary, self.conf['realm'])

        r = self.execute('kadmin.local -d "%s" -q "addprinc -randkey %s"' %
                         (self.conf.get('dbpath'), principal)).wait()
        if r != 0:
            raise Exception('ktadmin addprinc failed')

        # Generate keytab
        keytabdir = self.create_dir(os.path.join('keytabs', primary))
        if instance is not None:
            keytab = self.mkpath(os.path.join(keytabdir, instance))
        else:
            keytab = self.mkpath(os.path.join(keytabdir, "default"))

        r = self.execute('kadmin.local -d "%s" -q "ktadd -k "%s" %s"' %
                         (self.conf.get('dbpath'), keytab, principal)).wait()
        if r != 0:
            raise Exception('ktadmin ktadd failed')

        # Return keytab path
        return (principal, keytab)

    @staticmethod
    def add_cross_realm_tgts(kdcs):
        """ Add cross-realm TGTs.
            kdcs is a dict indexed by realm name, value is KerberosKdcApp. """
        realms = kdcs.keys()
        for realm in realms:
            for crealm in [x for x in realms if x != realm]:
                kdcs[realm].execute('kadmin.local -d "{}" -q "addprinc -requires_preauth -pw password krbtgt/{}@{}"'.format(kdcs[realm].conf.get('dbpath'), crealm, realm)).wait()  # noqa: E501
                kdcs[realm].execute('kadmin.local -d "{}" -q "addprinc -requires_preauth -pw password krbtgt/{}@{}"'.format(kdcs[realm].conf.get('dbpath'), realm, crealm)).wait()  # noqa: E501
