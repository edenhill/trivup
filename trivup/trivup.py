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
import socket
import resource
import datetime
import sys


class Cluster (object):
    def __init__(self, name, root_path, nodes=['localhost'], debug=False):
        super(Cluster, self).__init__()
        self.debug = debug
        self.name = name
        self.instance = str(int(time.time()))[2:]
        self.nodes = dict()
        for n in nodes:
            self.nodes[n] = Node(n)

        if sys.platform == 'linux2':
            self.platform = 'linux'
        else:
            self.platform = sys.platform

        self.apps = list()

        self.root_path = os.path.join(os.path.abspath(root_path), name)

        self.appid_next = 1
        # Allocated TcpPortAllocator ports
        self.tcp_ports = dict()

    def log(self, msg):
        print('[%s] %s: %s' % (datetime.datetime.now(), self.name, msg))

    def dbg(self, msg):
        if self.debug:
            return self.log(msg)

    def find_node(self, nodename):
        return self.nodes.get(nodename, None)

    def get_node(self):
        """ Returns a node """
        for node in self.nodes:
            return self.nodes[node]

    def add_app(self, app):
        self.apps.append(app)

    def find_app(self, appclass, by_conf=None):
        """ Return an app instance matching appclass (string or type).
            If by_conf is set to a (name,value) tuple, the application's
            config property 'name' must have the value of 'value'.
        """
        for app in self.apps:
            if type(appclass) == str:
                if app.__class__.__name__ != appclass:
                    continue
            elif not isinstance(app, appclass):
                continue

            if by_conf is not None:
                if app.conf.get(by_conf[0], None) != by_conf[1]:
                    continue
            return app

        return None

    def find_apps(self, appclass, in_state=None):
        """ Returns a list of app instances matching appclass (type). """
        apps = []
        for app in self.apps:
            if not isinstance(app, appclass):
                continue

            if in_state is not None and app.state != in_state:
                continue

            apps.append(app)

        return apps

    def deploy(self):
        """ @brief Deploy all apps in cluster """
        for app in self.apps:
            app.deploy()

    def start(self, timeout=None):
        """
        Start all apps in cluster
        @param timeout float/None: Number of seconds to wait for cluster to
                                   go operational, raises an
                                   Exception on failure.
        """
        for app in self.apps:
            if app.autostart and app.status() != 'started':
                app.start()
            if timeout is not None and not self.wait_operational(timeout):
                raise Exception('Cluster did not go operational in %ds' %
                                timeout)

    def stop(self, force=False):
        """ Stop all apps in cluster """
        for app in reversed(self.apps):
            app.stop(force=force)

    def cleanup(self, keeptypes=['perm', 'log']):
        for app in reversed(self.apps):
            app.cleanup(keeptypes=keeptypes)

    def run_post_cmds(self):
        """
        Run any registered post_start_cmds for all apps.
        Should only be called once when the cluster goes fully operational.
        """
        for app in self.apps:
            app.run_post_cmds()

    def wait_stopped(self, timeout=30):
        """ Wait for all apps to stop """
        t_end = time.time() + timeout
        while time.time() < t_end:
            not_stopped = [x for x in self.apps if x.status() != 'stopped']
            if len(not_stopped) == 0:
                return True
            self.dbg('Waiting for %d apps to stop: %s' %
                     (len(not_stopped),
                      ', '.join([str(x) for x in not_stopped])))
            time.sleep(1)
        return False

    def wait_operational(self, timeout=30):
        """ Wait for all started apps in the cluster to become operational """
        t_end = time.time() + timeout
        while time.time() < t_end:
            not_oper = [x for x in self.apps
                        if x.status() == 'started' and not x.operational()]
            stopped = [x for x in self.apps if x.status() == 'stopped']
            if len(not_oper) == 0:
                if len(stopped) > 0:
                    self.log(('%d apps terminated while waiting '
                              'to go operational: %s') %
                             (len(stopped),
                              ', '.join([str(x) for x in stopped])))
                    return False
                # Run post_start_cmds for all apps
                self.run_post_cmds()
                return True
            self.dbg('Waiting for %d apps to go operational: %s' %
                     (len(not_oper), ', '.join([str(x) for x in not_oper])))
            time.sleep(1)
        return False

    def get_all(self, key, defval=None, match_class=None):
        """ Retrieve key from all apps, return as list. """
        return [x.get(key, defval) for x in self.apps
                if isinstance(x, match_class)]

    def instance_path(self):
        """ Returns the instance path """
        return os.path.join(self.root_path, self.instance)

    def mkpath(self, relpath, unique=False, in_instance=False):
        """ Cluster-wide path: will not be cleaned up. """
        if in_instance:
            path = os.path.join(self.root_path, self.instance, relpath)
        else:
            path = os.path.join(self.root_path, relpath)
        if unique is True:
            path += '.' + str(uuid4())
        return path

    def get_docker_host(self):
        """ Get the docker host's name or IP from a container's point of view.
            On OSX we use the handy host.docker.internal hostname, on Linux
            we assume the docker container runs with --network=host and thus
            has access to localhost ports. """
        if self.platform == 'darwin':
            return 'host.docker.internal'
        elif self.platform == 'linux':
            return 'localhost'

        raise Exception("Unsupported platform: {}".format(self.platform))


class Allocator (object):
    def __init__(self, cluster):
        super(Allocator, self).__init__()
        self.cluster = cluster

    def next(self, app):
        appid = self.cluster.appid_next
        self.cluster.appid_next += 1
        return appid


class TcpPortAllocator (Allocator):
    def next(self, app, port_base=None):
        """ Let the kernel allocate a port number by opening a TCP socket,
            then closing it and return the port number.
            Linux tries to avoid returning the same port again, so this should
            work...
        """
        if port_base is not None:
            port = port_base
        else:
            port = 0

        for i in range(1, 100):
            s = None
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.bind(('', port))
                port = s.getsockname()[1]
                s.close()
            except Exception:
                if s is not None:
                    s.close()
                if port_base is not None:
                    port += 1
                    continue
                raise
            if self.cluster.tcp_ports.get(port, None) is not None:
                port += 1
                continue
            self.cluster.tcp_ports[port] = app
            return port

        raise Exception(("Could not allocate port (port_base=%s) "
                         "in 100 attempts") % port_base)


class UuidAllocator (Allocator):
    @staticmethod
    def _next(trunc=36):
        return str(uuid4())[:trunc]

    def next(self, app, trunc=36):
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
        self.appid = Allocator(cluster).next(self)
        self.name = self.__class__.__name__
        self.cluster = cluster
        self.autostart = True  # Starts with cluster.start()
        self.do_cleanup = True
        # Environment variables applied to execution
        self.env = defaultdict(list)
        self.env_add('LC_ALL', 'C')
        # List {'type': .., 'path': ..} tuples of created paths, for cleanup
        self.paths = list()
        self.debug = cluster.debug
        # Post-startup commands
        self.post_start_cmds = list()

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

        self.conf['appid'] = self.appid
        self.conf['name'] = self.name
        self.conf['nodename'] = self.node.name

        if 'version' not in self.conf:
            self.conf['version'] = 'master'

        # Runtime root path (runtime created files)
        self._root_path = os.path.join(cluster.root_path, cluster.instance,
                                       self.name)

        # Create root path dir
        self.create_dir('')

        self.state = 'init'
        self.dbg('Creating %s instance' % self.name)

    def log(self, msg):
        print('[%s] %s-%s: %s' %
              (datetime.datetime.now(), self.name, self.appid, msg))

    def dbg(self, msg):
        if self.debug:
            return self.log(msg)

    def get(self, key, defval=None):
        """ Return conf value for @param key, or @param defval if not found."""
        return self.conf.get(key, defval)

    def root_path(self):
        return os.path.join(self._root_path, str(self.appid))

    def add_path(self, relpath, pathtype):
        """ Add path for future use by cleanup() et.al. """
        self.paths.append({'path': relpath, 'type': pathtype})
        return relpath

    def mkpath(self, relpath, pathtype='temp', unique=False):
        """ pathtype := perm, temp, log """
        path = os.path.join(self.root_path(), relpath)
        if unique is True:
            path += '.' + str(uuid4())
        self.add_path(path, pathtype)
        return path

    def create_dir(self, relpath, unique=False):
        path = self.mkpath(relpath, unique=unique)
        if not os.path.exists(path):
            os.makedirs(path)
        return path

    def open_file(self, relpath, unique=False, pathtype='temp'):
        path = self.mkpath(relpath, unique=unique, pathtype=pathtype)
        basename = os.path.dirname(path)
        if not os.path.exists(basename):
            os.makedirs(basename)
        f = open(path, 'wb')

        return f, path

    def create_file(self, relpath, unique=False, data=None, pathtype='temp'):
        f, path = self.open_file(relpath, unique=unique, pathtype=pathtype)
        if data is not None:
            if type(data) == str:
                data = data.encode('ascii')
            f.write(data)
        f.close()
        return path

    def create_file_from_template(self, relpath, unique=False,
                                  template_name=None, append_data=None,
                                  subst=True, pathtype='temp'):
        """ Create file from app template using app's conf dict.
            If subst=False no template operations will be performed
            and the file is copied verbatim. """
        if not template_name:
            tname = template_name = os.path.basename(relpath)
        else:
            tname = template_name

        # Try pkgutil resource locator
        tpath = os.path.join('apps', self.__class__.__name__,
                             tname + '.template')
        filedata = pkgutil.get_data('trivup', tpath)
        if filedata is None:
            raise IOError('Class %s resource %s not found' % ('trivup', tpath))

        if subst:
            rendered = Template(filedata.decode('ascii')).substitute(self.conf)
        else:
            rendered = filedata.decode('ascii')
        if append_data is not None:
            rendered += '\n' + append_data + '\n'
        return self.create_file(relpath, unique, data=rendered,
                                pathtype=pathtype)

    def resource_path(self, relpath):
        """ @returns the full path to an application class resource file """
        return pkg_resources.resource_filename('trivup',
                                               os.path.join('apps', self.__class__.__name__, relpath))  # noqa: E501

    def env_add(self, name, value, append=True):
        """ Add (overwrite or append) environment variable """
        if name in self.env and append:
            self.env[name] += ' %s' % value
        else:
            self.env[name] = value

    def start_cmd(self):
        """ @return Command line to start application. """
        return self.conf['start_cmd']

    def execute(self, cmd, stdout_fd=None, stderr_fd=None):
        """
        Execute command, returns the subprocess handle

        @param stdout_fd, stderr_fd: either None (for no redirect), a fd,
                                     or a string (to open and append to file)
        """
        cmd = self.node.exec_cmd + cmd
        self.dbg('Executing: %s' % cmd)
        self.dbg('Environment: %s' % str(self.env))

        fdlimit = self.conf.get('fdlimit', 0)
        if fdlimit == 'max':
            try:
                soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
                fdlimit = hard
            except Exception as e:
                self.log(('Failed to get RLIMIT_NOFILE: {}: '
                          'using system default').format(e))
                fdlimit = 0
        self.dbg('FD limit: %d' % fdlimit)
        if fdlimit > 0:
            try:
                resource.setrlimit(resource.RLIMIT_NOFILE, (fdlimit, fdlimit))
            except ValueError as e:
                self.log('Failed to set RLIMIT_NOFILE({},{}): {}'.format(
                    fdlimit, fdlimit, e))

        to_close = list()
        if type(stdout_fd) == str:
            f = open(stdout_fd, 'a')
            stdout_fd = f.fileno()
            to_close.append(f)
        if type(stderr_fd) == str:
            f = open(stderr_fd, 'a')
            stderr_fd = f.fileno()
            to_close.append(f)

        f = open('/dev/null', 'r')
        stdin_fd = f.fileno()
        to_close.append(f)

        proc = subprocess.Popen(cmd, shell=True, preexec_fn=os.setsid,
                                env=dict(os.environ, **self.env),
                                stdout=stdout_fd, stderr=stderr_fd,
                                stdin=stdin_fd)
        for f in to_close:
            f.close()

        return proc

    def run(self):
        """ Run application using conf @param start_cmd.
            @returns true if app was started, or false if no start_cmd """
        start_cmd = self.start_cmd()
        if start_cmd is None:
            return False

        self.stdout_fd = open(self.mkpath('stdout.log', pathtype='log'), 'a')
        self.stderr_fd = open(self.mkpath('stderr.log', pathtype='log'), 'a')
        self.proc = self.execute(start_cmd,
                                 stdout_fd=self.stdout_fd,
                                 stderr_fd=self.stderr_fd)
        return True

    def run_post_cmds(self):
        """
        Run any registers post_start_cmds.
        Should only be called once when the cluster is operational.
        """
        self.dbg('Running %d post_start_cmds' % len(self.post_start_cmds))
        for cmd in self.post_start_cmds:
            try:
                output = subprocess.check_output(cmd,
                                                 env=dict(os.environ,
                                                          **self.env),
                                                 shell=True)
                self.dbg('%s returned: %s' % (cmd, output))
            except subprocess.CalledProcessError as e:
                self.log('Failed to run %s' % (cmd))
                raise e

        # Avoid re-run
        self.post_start_cmds = list()

    def start(self):
        if self.state == 'started':
            raise Exception('%s already started' % self.name)

        if self.run():
            self.state = 'started'
            self.t_started = time.time()

    def pid(self):
        if self.proc is None:
            return 0
        return self.proc.pid

    def wait_stopped(self, timeout=30, force=False):
        """
        Wait for process to terminate.
        Application .state will be updated.

        @param force bool: Force process termination after @param timeout
        @param timeout float
        @returns True on succesful termination, else False.
        """
        t_end = time.time() + timeout
        while time.time() < t_end and (self.proc.poll() is None or
                                       self.proc.returncode is None):
            time.sleep(0.5)

        r = self.proc.wait()
        self.dbg("wait {} returned {}, wait_stopped returncode {}".format(
            self.proc.pid, r, self.proc.returncode))

        if self.proc.returncode is None:
            self.dbg('still alive')
            if force:
                self.dbg('forcing termination')
                os.killpg(os.getpgid(self.proc.pid), signal.SIGKILL)
                self.proc.wait()
            else:
                self.log('process did not terminate in %ds' % timeout)
                self.state = 'stale'
                return False

        self.state = 'stopped'
        self.t_stopped = time.time()
        return True

    def stop(self, wait_term=True, force=False):
        if self.state != 'started':
            return

        self.dbg('Stopping (pid {})'.format(self.proc.pid))
        try:
            os.killpg(os.getpgid(self.proc.pid), signal.SIGTERM)
        except OSError as e:
            self.log('killpg() failed: already dead? (%s): ignoring' % str(e))
            wait_term = False

        if wait_term:
            # Wait for termination
            self.wait_stopped(timeout=10, force=force)
        else:
            self.state = 'stopped'

        self.dbg('now %s, runtime %ds' % (self.state, self.runtime()))

        self.stdout_fd.close()
        self.stderr_fd.close()
        self.proc = None

    def status(self):
        if self.state == 'started' and self.proc is not None and self.proc.poll() is not None:  # noqa: E501
            r = self.proc.wait()
            if r != 0:
                self.log('process terminated: returncode %s' % (str(r)))
            else:
                self.dbg('process terminated: returncode %s' % (str(r)))
            self.state = 'stopped'
        return self.state

    def operational(self):
        return True  # Positive dummy: should be implemented by subclass

    def wait_operational(self, timeout=30):
        """ Wait for application to go operational """
        t_end = time.time() + timeout
        while time.time() < t_end:
            if self.status() == 'stopped':
                return False
            if self.operational():
                self.run_post_cmds()
                return True
            time.sleep(1.0)
        return False

    def deploy(self):
        """ Deploy application on node. NOT IMPLEMENTED """
        raise NotImplementedError('Deploy not implemented')

    def cleanup(self, keeptypes=['perm', 'log']):
        """ Remove all dirs and files created by App """
        if not self.do_cleanup:
            return
        self.dbg('Cleaning up %d path(s) (keeptypes=%s)' %
                 (len(self.paths), ','.join(keeptypes)))
        for p in self.paths:
            path = p['path']
            if not os.path.exists(path):
                continue
            self.dbg('Cleanup: %s (type %s, %s)' % (path, p['type'], p))
            if keeptypes is not None and p['type'] in keeptypes:
                continue
            try:
                if os.path.isdir(path):
                    shutil.rmtree(path)
                else:
                    os.remove(path)
            except Exception as e:
                self.dbg('Remove %s failed: %s: ignoring' % (path, str(e)))

    def runtime(self):
        if self.t_stopped < 1:
            return time.time() - self.t_started
        else:
            return self.t_stopped - self.t_started

    def __str__(self):
        return '{%s@%s:%s(%s)}' % (self.name, self.node.name,
                                   self.appid, self.state)


if __name__ == '__main__':
    pass
