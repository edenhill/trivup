#!/usr/bin/env python

from trivup.trivup import Cluster
from trivup.apps.SslApp import SslApp


def test_Ssl():
    cluster = Cluster('TestCluster', 'tmp', debug=True)

    # SSL App
    ssl = SslApp(cluster)

    a = ssl.create_keystore('mybroker')
    print('created keystore: %s' % (a,))

    r = ssl.create_cert('myclient')
    print('created key: %s' % r)

    r = ssl.create_cert('selfsigned_myclient', with_ca=False)
    print('created key: %s' % r)

    r = ssl.create_cert('intermediate_myclient', through_intermediate=True)
    print('created key: %s' % r)

    r = ssl.create_cert('selfsigned_intermediate_myclient',
                        with_ca=False, through_intermediate=True)
    print('created key: %s' % r)

    cluster.cleanup()
