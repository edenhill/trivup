#!/usr/bin/env python

from trivup.trivup import Cluster
from trivup.apps.SslApp import SslApp


if __name__ == '__main__':
    cluster = Cluster('TestCluster', 'tmp', debug=True)

    # SSL App
    ssl = SslApp(cluster)

    a = ssl.create_keystore('mybroker')
    print('created keystore: %s' % a)

    r = ssl.create_cert('myclient')
    print('created key: %s' % r)

    cluster.cleanup(keeptypes=[])
