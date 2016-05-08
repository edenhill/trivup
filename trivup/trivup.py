

import os
import signal
from string import Template
from uuid import uuid4
from copy import deepcopy
from collections import defaultdict
import subprocess
import shutil
import time
import pkgutil
import pkg_resources
import random
import socket

class Cluster (object):
    def __init__(self, name, root_path, nodes=['localhost']):
        super(Cluster, self).__init__()
        self.name = name
        self.instance = str(int(time.time()))[2:]
        self.nodes = dict()
        for n in nodes:
            self.nodes[n] = Node(n)

        self.apps = list()

        self.root_path = os.path.join(os.path.abspath(root_path), name)

        self.appid_next = 1

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

    def start (self, timeout=None):
        """
        Start all apps in cluster
        @param timeout float/None: Number of seconds to wait for cluster to
                                   go operational, raises an
                                   Exception on failure.
        """
        for app in self.apps:
            if app.autostart:
                app.start()
            if timeout is not None and not self.wait_operational(timeout):
                raise Exception('Cluster did not go operational in %ds' % timeout)


    def stop (self, force=False):
         """ Stop all apps in cluster """
         for app in reversed(self.apps):
             app.stop(force=force)

    def cleanup (self, keeptypes=['perm','log']):
        for app in reversed(self.apps):
            app.cleanup(keeptypes=keeptypes)

    def wait_operational (self, timeout=30):
        """ Wait for all started apps in the cluster to become operational """
        t_end = time.time() + timeout
        while time.time() < t_end:
            not_oper = [x for x in self.apps if x.status() == 'started' and not x.operational()]
            stopped = [x for x in self.apps if x.status() == 'stopped']
            if len(not_oper) == 0:
                if len(stopped) > 0:
                    print('### %d apps terminated while waiting to go operational: %s' % \
                          (len(stopped), ', '.join([str(x) for x in stopped])))
                    return False
                return True
            print('# Waiting for %d apps to go operational: %s' %
                  (len(not_oper), ', '.join([str(x) for x in not_oper])))
            time.sleep(1)
        return False

    def get_all (self, key, defval=None, match_class=None):
        """ Retrieve key from all apps, return as list. """
        return [x.get(key, defval) for x in self.apps if isinstance(x, match_class)]

    def mkpath (self, relpath, unique=False):
        """ Cluster-wide path: will not be cleaned up. """
        path = os.path.join(self.root_path, relpath)
        if unique is True:
            path += '.' + str(uuid4())
        return path


class Allocator (object):
    def __init__ (self, cluster):
        super(Allocator, self).__init__()
        self.cluster = cluster

    def next (self):
        appid = self.cluster.appid_next
        self.cluster.appid_next += 1
        return appid


class TcpPortAllocator (Allocator):
    def next (self):
        """ Let the kernel allocate a port number by opening a TCP socket,
            then closing it and return the port number.
            Linux tries to avoid returning the same port again, so this should
            work...
        """
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind(('', 0))
        port = s.getsockname()[1]
        s.close()
        return port

class UuidAllocator (Allocator):
    @staticmethod
    def _next (trunc=36):
        return str(uuid4())[:trunc]

    def next (self, trunc=36):
        return self._next(trunc=trunc)


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
        self.appid = Allocator(cluster).next()
        self.name = self.__class__.__name__
        self.cluster = cluster
        self.autostart = True # Starts with cluster.start()
        self.do_cleanup = True
        # Environment variables applied to execution
        self.env = defaultdict(list)
        # List {'type': .., 'path': ..} tuples of created paths, for cleanup
        self.paths = list()

        self.t_started = 0
        self.t_stopped = 0

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
        self._root_path = os.path.join(cluster.root_path, cluster.instance,
                                       self.name)
        self.state = 'init'
        self.log('Creating %s instance' % self.name)
        

    def log (self, msg):
        print('%s-%s: %s' % (self.name, self.appid, msg))

    def get(self, key, defval=None):
        """ Return conf value for \p key, or \p defval if not found. """
        return self.conf.get(key, defval)

    def root_path (self):
        return os.path.join(self._root_path, str(self.appid))

    def add_path (self, relpath, pathtype):
        """ Add path for future use by cleanup() et.al. """
        self.paths.append({'path': relpath, 'type': pathtype})

    def mkpath (self, relpath, pathtype='temp', unique=False):
        """ pathtype := perm, temp, log """
        path = os.path.join(self.root_path(), relpath)
        if unique is True:
            path += '.' + str(uuid4())
        self.add_path(path, pathtype)
        return path

    def create_dir (self, relpath, unique=False):
        path = self.mkpath(relpath, unique)
        if not os.path.exists(path):
            os.makedirs(path)
        return path

    def open_file (self, relpath, unique=False, pathtype='temp'):
        path = self.mkpath(relpath, unique=unique, pathtype=pathtype)
        basename = os.path.dirname(path)
        if not os.path.exists(basename):
            os.makedirs(basename)
        f = open(path, 'wb')

        return f, path

    def create_file (self, relpath, unique=False, data=None, pathtype='temp'):
        f, path = self.open_file(relpath, unique=unique, pathtype=pathtype)
        if data is not None:
            if type(data) == str:
                data = data.encode('ascii')
            f.write(data)
        f.close()
        return path

    def create_file_from_template (self, relpath, unique=False, template_name=None, append_data=None, subst=True, pathtype='temp'):
        """ Create file from app template using app's conf dict.
            If subst=False no template operations will be performed and the file is copied verbatim. """
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

        if subst:
            rendered = Template(filedata.decode('ascii')).substitute(self.conf)
        else:
            rendered = filedata.decode('ascii')
        if append_data is not None:
            rendered += '\n' + append_data
        return self.create_file(relpath, unique, data=rendered, pathtype=pathtype)


    def resource_path (self, relpath):
        """ @returns the full path to an application class resource file """
        return pkg_resources.resource_filename('trivup',
                                               os.path.join('apps', self.__class__.__name__, relpath))

    def env_add (self, name, value, append=True):
        """ Add (overwrite or append) environment variable """
        if name in self.env and append:
            self.env[name] += ' %s' % value
        else:
            self.env[name] = value

    def start_cmd (self):
        """ @return Command line to start application. """
        return self.conf['start_cmd']

    def run (self):
        """ Run application using conf \p start_cmd """
        cmd = self.node.exec_cmd + self.start_cmd()
        self.log('Starting: %s' % cmd)
        self.log('Environment: %s' % str(self.env))
        self.stdout_fd = open(self.mkpath('stdout.log', 'log'), 'a')
        self.stderr_fd = open(self.mkpath('stderr.log', 'log'), 'a')
        self.proc = subprocess.Popen(cmd, shell=True, preexec_fn=os.setsid,
                                     env=dict(os.environ, **self.env),
                                     stdout=self.stdout_fd, stderr=self.stderr_fd)
        
    def start (self):
        if self.state == 'started':
            raise Exception('%s already started' % self.name)

        self.run()
        self.state = 'started'
        self.t_started = time.time()

    def wait_stopped (self, timeout=30, force=False):
        """
        Wait for process to terminate.
        Application .state will be updated.

        @param force bool: Force process termination after \p timeout
        @param timeout float
        @returns True on succesful termination, else False.
        """
        t_end = time.time() + timeout
        while time.time() < t_end and self.proc.poll() is None:
            time.sleep(0.5)

        if self.proc.poll() is None:
            self.log('still alive')
            if force:
                self.log('forcing termination')
                os.killpg(os.getpgid(self.proc.pid), signal.SIGKILL)
                self.proc.wait(1)
            else:
                self.log('process did not terminate in %ds' % timeout)
                self.state = 'stale'
                return False

        self.state = 'stopped'
        self.t_stopped = time.time()
        return True

    def stop (self, wait_term=True, force=False):
        if self.state != 'started':
            return

        self.log('Stopping')
        os.killpg(os.getpgid(self.proc.pid), signal.SIGTERM)

        if wait_term:
            # Wait for termination
            self.wait_stopped(timeout=10, force=force)
        else:
            self.state = 'stopped'

        self.log('now %s, runtime %ds' % (self.state, self.runtime()))
        
        self.stdout_fd.close()
        self.stderr_fd.close()
        self.proc = None


    def status (self):
        if self.state == 'started' and self.proc is not None and self.proc.poll() is not None:
            r = self.proc.wait()
            self.log('process terminated: returncode %s' % (str(r)))
            self.state = 'stopped'
        return self.state

    def operational (self):
        return True # Positive dummy: should be implemented by subclass

    def wait_operational (self, timeout=30):
        """ Wait for application to go operational """
        t_end = time.time() + timeout
        while time.time() < t_end:
            if self.status() == 'stopped':
                return False
            if self.operational():
                return True
            time.sleep(1.0)
        return False


    def deploy (self):
        """ Deploy application on node. NOT IMPLEMENTED """
        raise NotImplementedError('Deploy not implemented')

    def cleanup (self, keeptypes=['perm','log']):
        """ Remove all dirs and files created by App """
        if not self.do_cleanup:
            return
        self.log('Cleaning up %d path(s) (keeptypes=%s)' %
                 (len(self.paths), ','.join(keeptypes)))
        for p in self.paths:
            path = p['path']
            if not os.path.exists(path):
                continue
            if keeptypes is not None and p['type'] in keeptypes:
                continue
            self.log('Cleanup: %s' % path)
            try:
                if os.path.isdir(path):
                    shutil.rmtree(path)
                else:
                    os.remove(path)
            except Exception as e:
                self.log('Remove %s failed: %s: ignoring' % (path, str(e)))

    def runtime (self):
        if self.t_stopped < 1:
            return time.time() - self.t_started
        else:
            return self.t_stopped - self.t_started

    def __str__ (self):
        return '{%s@%s:%s(%s)}' % (self.name, self.node.name, self.appid, self.state)



if __name__ == '__main__':
    pass
