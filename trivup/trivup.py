

import os
import signal
from string import Template
from uuid import uuid1
from copy import deepcopy
import subprocess
import shutil
import time
import pkgutil
import pkg_resources
import random

class Cluster (object):
    def __init__(self, name, root_path, nodes=['localhost']):
        super(Cluster, self).__init__()
        self.name = name
        self.nodes = dict()
        for n in nodes:
            self.nodes[n] = Node(n)

        self.apps = list()

        self.root_path = os.path.join(os.path.abspath(root_path), name)

        # Generate random appids between runs to avoid TCP ports
        # already in use by TIME_WAIT connections.
        self.appid_next = 19000 + (10 * random.randint(0, 90))

    def find_node (self, nodename):
        return self.nodes.get(nodename, None)

    def get_node (self):
        """ Returns a node """
        for node in self.nodes:
            return self.nodes[node]


    def add_app (self, app):
        self.apps.append(app)


    def find_app (self, appclass):
        """ Return an app instance matching appclass (string or type) """
        for app in self.apps:
            if type(appclass) == str:
                if app.__class__.__name__ == appclass:
                    return app
            elif isinstance(app, appclass):
                return app

    def deploy (self):
        """ @brief Deploy all apps in cluster """
        for app in self.apps:
            app.deploy()

    def start (self):
        """ Start all apps in cluster """
        for app in self.apps:
            app.start()


    def stop (self, force=False):
         """ Stop all apps in cluster """
         for app in reversed(self.apps):
             app.stop(force=force)

    def cleanup (self, keeptypes=['perm']):
        for app in reversed(self.apps):
            app.cleanup(keeptypes=keeptypes)

    def wait_operational (self, timeout=30):
        """ Wait for all started apps in the cluster to become operational """
        t_end = time.time() + timeout
        while time.time() < t_end:
            not_oper = [x for x in self.apps if x.status() == 'started' and not x.operational()]
            if len(not_oper) == 0:
                return True
            print('# Waiting for %d apps to go operational: %s' %
                  (len(not_oper), ', '.join([str(x) for x in not_oper])))
            time.sleep(1)
        return False

    def get_all (self, key, defval=None, match_class=None):
        """ Retrieve key from all apps, return as list. """
        return [x.get(key, defval) for x in self.apps if isinstance(x, match_class)]
            

class Allocator (object):
    def __init__ (self, cluster):
        super(Allocator, self).__init__()
        self.cluster = cluster

class TcpPortAllocator (Allocator):
    def __init__ (self, cluster):
        super(TcpPortAllocator, self).__init__(cluster)

    def next (self):
        appid = self.cluster.appid_next
        self.cluster.appid_next += 1
        return appid


class Node (object):
    def __init__(self, name):
        super(Node, self).__init__()
        self.name = name
        if name == 'localhost':
            self.exec_cmd = ''
        else:
            self.exec_cmd = 'ssh %s ' % name


class App (object):
    def __init__(self, cluster, conf=None, on=None):
        self.appid = None
        self.name = self.__class__.__name__
        self.cluster = cluster
        # List {'type': .., 'path': ..} tuples of created paths, for cleanup
        self.paths = list()

        if on:
            self.node = cluster.find_node(on)
        else:
            self.node = cluster.get_node()
        if self.node is None:
            raise Exception('No node available for %s' % self)

        cluster.add_app(self)
        if conf is None:
            self.conf = dict()
        else:
            self.conf = deepcopy(conf)

        self.conf['name'] = self.name
        self.conf['nodename'] = self.node.name

        if 'version' not in self.conf:
            self.conf['version'] = 'master'
       
        # Runtime root path (runtime created files)
        self.root_path = os.path.join(cluster.root_path, self.name)
        self.state = 'init'
        self.log('Creating %s instance' % self.name)
        

    def log (self, msg):
        print('%s-%s: %s' % (self.name, self.appid, msg))

    def get(self, key, defval=None):
        """ Return conf value for \p key, or \p defval if not found. """
        return self.conf.get(key, defval)

    def add_path (self, relpath, pathtype):
        """ Add path for future use by cleanup() et.al. """
        self.paths.append({'path': relpath, 'type': pathtype})

    def mkpath (self, relpath, pathtype='temp', unique=False):
        """ pathtype := perm, temp, log """
        path = os.path.join(self.root_path, str(self.appid), relpath)
        if unique is True:
            path += '.' + str(uuid1())
        self.add_path(path, pathtype)
        return path

    def create_dir (self, relpath, unique=False):
        path = self.mkpath(relpath, unique)
        if not os.path.exists(path):
            os.makedirs(path)
        return path

    def create_file (self, relpath, unique=False, data=None):
        path = self.mkpath(relpath, unique)
        basename = os.path.dirname(path)
        if not os.path.exists(basename):
            os.makedirs(basename)
        with open(path, 'w') as f:
            if data is not None:
                f.write(data)

        return path

    def create_file_from_template (self, relpath, unique=False, template_name=None):
        if not template_name:
            tname = template_name = os.path.basename(relpath)
        else:
            tname = template_name

        # Try pkgutil resource locator
        tpath = os.path.join('apps', self.__class__.__name__,
                             tname + '.template')
        filedata = pkgutil.get_data('trivup', tpath)
        if filedata is None:
            raise FileNotFoundError('Class %s resource %s not found' %
                                    ('trivup', tpath))

        rendered = Template(filedata).substitute(self.conf)
        return self.create_file(relpath, unique, data=rendered)


    def resource_path (self, relpath):
        """ @returns the full path to an application class resource file """
        return pkg_resources.resource_filename('trivup',
                                               os.path.join('apps', self.__class__.__name__, relpath))


    def run (self):
        cmd = self.node.exec_cmd + self.conf['start_cmd']
        self.log('Starting: %s' % cmd)
        self.stdout_fd = open(self.mkpath('stdout.log', 'log'), 'a')
        self.stderr_fd = open(self.mkpath('stderr.log', 'log'), 'a')
        self.proc = subprocess.Popen(cmd, shell=True, preexec_fn=os.setsid,
                                     stdout=self.stdout_fd, stderr=self.stderr_fd)
        
    def start (self):
        if self.state == 'started':
            raise Exception('%s already started' % self.name)

        self.run()
        self.state = 'started'

    def stop (self, wait_term=True, force=False):
        if self.state != 'started':
            return

        self.log('Stopping')
        os.killpg(os.getpgid(self.proc.pid), signal.SIGTERM)

        if wait_term:
            # Wait for termination
            t_end = time.time() + 10
            while time.time() < t_end and self.proc.poll() is None:
                time.sleep(0.5)
                
            if self.proc.poll() is None:
                self.log('still alive')
                if force:
                    self.log('forcing termination')
                    os.killpg(os.getpgid(self.proc.pid), signal.SIGKILL)
                    self.proc.wait(0.1)
                    self.state = 'stopped'
                else:
                    self.state = 'stale'

            else:
                self.state = 'stopped'
        else:
            self.state = 'stopped'

        self.log('now %s' % self.state)
        
        self.stdout_fd.close()
        self.stderr_fd.close()
        self.proc = None


    def status (self):
        if self.state == 'started' and self.proc is not None and self.proc.poll() is not None:
            self.state = 'stopped'
        return self.state

    def operational (self):
        return True # Positive dummy: should be implemented by subclass

    def wait_operational (self, timeout=30):
        """ Wait for application to go operational """
        t_end = time.time() + timeout
        while time.time() < t_end:
            if self.operational():
                return True
            time.sleep(1.0)
        return False


    def deploy (self):
        """ Deploy application on node. NOT IMPLEMENTED """
        raise NotImplementedError('Deploy not implemented')

    def cleanup (self, keeptypes=['perm','log']):
        """ Remove all dirs and files created by App """
        for p in self.paths:
            path = p['path']
            if not os.path.exists(path):
                continue
            if keeptypes is not None and p['type'] in keeptypes:
                continue
            self.log('Cleanup: %s' % path)
            if os.path.isdir(path):
                shutil.rmtree(path)
            else:
                os.remove(path)

    def __str__ (self):
        return '{%s@%s:%s(%s)}' % (self.name, self.node.name, self.appid, self.state)



if __name__ == '__main__':
    pass
