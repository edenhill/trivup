#!/usr/bin/env python

from trivup.trivup import Cluster
from trivup.apps.SslApp import SslApp

import subprocess
import time


if __name__ == '__main__':
    cluster = Cluster('TestCluster', 'tmp', debug=True)

    # SSL App
    ssl = SslApp(cluster)

    a = ssl.create_keystore('mybroker')
    print('created keystore: %s' % str(a))

    b = ssl.create_key('myclient')
    print('created key: %s' % str(b))

    cluster.cleanup(keeptypes=[])

