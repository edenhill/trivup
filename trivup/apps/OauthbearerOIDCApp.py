#!/usr/bin/env python
#

# Copyright (c) 2021, Magnus Edenhill
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
from http.server import BaseHTTPRequestHandler, HTTPServer

from jwcrypto import jwk
import python_jwt as jwt
from datetime import datetime, timedelta
from threading import Lock

import json
import argparse
import requests


class WebServerHandler(BaseHTTPRequestHandler):
    def __init__(self):
        self._key = None
        self._public_keys = []
        self._mutex = Lock()

    def __call__(self, *args, **kwargs):
        """
        As it turns out, http.server.BaseHTTPRequestHandler
        ultimately inherits from socketserver.BaseRequestHandler,
        and socketserver.BaseRequestHandler.__init__() calls do_GET()
        and do_POST(). So if we add the super().__init__(*args, **kwargs)
        to the constructor of WebServerHandler, it means all the
        instance variables are initialized for each request.
        But the relevant parts of the JWT are cached in the broker.
        The only time the broker communicates with the OAuth/OIDC
        token provider is at a) startup, b) during a scheduled refresh,
        and c) when it finds a key ID that isn't cached.
        So we want to keep the same public key for each user in case
        the broker won't get the new public key immediately.
        So this method keeps the superclass "constructor"
        call out of "constructor" of this class which eliminates the
        possibility of dispatching a request (from the superclass's
        constructor) before the subclass's constructor is finished
        to avoid initializing the instance variables.
        Refer to https://stackoverflow.com/a/58909293
        """
        super().__init__(*args, **kwargs)

    def update_keys(self):
        self._mutex.acquire()
        if len(self._public_keys) == 0:
            public_key, key = self.generate_public_key()
            self._public_keys.append(json.loads(public_key))
            self._key = key
        self._mutex.release()

    def do_GET(self):
        if not self.path.endswith("/keys"):
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            message = "HTTP server for OAuth\n"
            message += "Example for token retrieval:\n"
            message += 'curl \
            -X POST \
            --url localhost:PORT/retrieve \
            -H "Accept: application/json" \
            -H "Content-Type: application/x-www-form-urlencoded" \
            -H "Authorization: Basic LW4gYWJjMTIzOlMzY3IzdCEK \
                (base64 string generated from CLIENT_ID:CLIENT_SECRET)" \
            -d "grant_type=client_credentials,scope=test-scope"'
            self.wfile.write(message.encode())
            return

        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        self.update_keys()
        keys = {"keys": self._public_keys}
        self.wfile.write(json.dumps(keys, indent=4).encode())

    def generate_token(self, lifetime, valid=True):
        self.update_keys()
        private_pem = self._key.export_to_pem(private_key=True, password=None)
        private_key = jwk.JWK.from_pem(private_pem)

        payload = {
            'exp': datetime.utcnow() + timedelta(days=0, seconds=lifetime),
            'iat': datetime.utcnow(),
            'iss': "issuer",
            'sub': "subject",
            'aud': 'api://default'
        }
        header = {
            "kid": "abcdefg"
        }

        token = jwt.generate_jwt(payload, private_key, 'RS256',
                                 timedelta(seconds=lifetime),
                                 other_headers=header)
        if not valid:
            token += "invalid"

        token_map = {"access_token": "%s" % token}
        return token_map

    def generate_public_key(self):

        key = jwk.JWK.generate(kty='RSA', size=2048, alg='RS256',
                               use='sig', kid="abcdefg")

        public_key = key.export_public()
        return (public_key, key)

    def generate_valid_token_for_client(self):
        """
        Example usage:
        curl \
        -X POST \
        --url localhost:PORT/retrieve \
        -H "Accept: application/json" \
        -H "Content-Type: application/x-www-form-urlencoded" \
        -H "Authorization: Basic LW4gYWJjMTIzOlMzY3IzdCEK \
            (base64 string generated from CLIENT_ID:CLIENT_SECRET)"
        -d "grant_type=client_credentials,scope=test-scope"
        """
        if self.headers.get('Content-Length', None) is None:
            self.send_error(400, 'Content-Length field is required')
            return

        if self.headers.get('Content-Type', None) is None:
            self.send_error(400, 'Content-Type field is required')
            return

        content_length = int(self.headers['Content-Length'])
        post_data = self.rfile.read(content_length)

        if self.headers.get('Authorization', None) is None:
            self.send_error(400, 'Authorization field is required')
            return

        if self.headers.get('Accept', None) != "application/json":
            self.send_error(400, 'Accept field should be "application/json"')
            return

        if post_data is None:
            self.send_error(400,
                            'grant_type=client_credentials and scope \
                             fields are required in data')
            return

        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        token = self.generate_token(4)
        self.wfile.write(json.dumps(token, indent=4).encode())

    def generate_badformat_token_for_client(self):
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()

        token = self.generate_token(30, False)
        self.wfile.write(json.dumps(token, indent=4).encode())

    def generate_expired_token_for_client(self):
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()

        token = self.generate_token(-1)
        self.wfile.write(json.dumps(token, indent=4).encode())

    def do_POST(self):
        if self.path.endswith("/retrieve"):
            self.generate_valid_token_for_client()
        elif self.path.endswith("/retrieve/badformat"):
            self.generate_badformat_token_for_client()
        elif self.path.endswith("/retrieve/expire"):
            self.generate_expired_token_for_client()
        else:
            self.send_error(404, 'URL is not valid: %s' % self.path)


class OauthbearerOIDCHttpServer():
    def run_http_server(self, port):
        handler = WebServerHandler()
        server = HTTPServer(('localhost', port), handler)
        server.serve_forever()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Trivup Oauthbearer OIDC \
                                                  HTTP server')
    parser.add_argument('--port', type=int, dest='port',
                        required=True,
                        help='Port at which OauthbearerOIDCApp \
                              should be bound')
    args = parser.parse_args()

    http_server = OauthbearerOIDCHttpServer()
    http_server.run_http_server(args.port)


class OauthbearerOIDCApp (trivup.App):
    """ Oauth/OIDC app, run a http server """
    def __init__(self, cluster, conf=None, on=None):
        """
        @param cluster     Current cluster.
        @param conf        Configuration dict.
               port        Port at which OauthbearerOIDCApp should be bound
                           (optional). A (random) free port will be chosen
                           otherwise.
        @param on          Node name to run on.
        """
        super(OauthbearerOIDCApp, self).__init__(cluster, conf=conf, on=on)
        self.conf['port'] = trivup.TcpPortAllocator(self.cluster).next(
            self, port_base=self.conf.get('port', None))
        self.conf['valid_url'] = 'http://localhost:%d/retrieve' % \
            self.conf['port']
        self.conf['badformat_url'] = 'http://localhost:%d/retrieve/badformat' \
            % self.conf['port']
        self.conf['expired_url'] = 'http://localhost:%d/retrieve/expire' % \
            self.conf['port']
        self.conf['jwks_url'] = 'http://localhost:%d/keys' % self.conf['port']
        self.conf['sasl_oauthbearer_method'] = 'OIDC'
        self.conf['sasl_oauthbearer_client_id'] = '123'
        self.conf['sasl_oauthbearer_client_secret'] = 'abc'
        self.conf['sasl_oauthbearer_scope'] = 'test'
        self.conf['sasl_oauthbearer_extensions'] = \
            'ExtensionworkloadIdentity=develC348S,Extensioncluster=lkc123'

    def start_cmd(self):
        return "python3 -m trivup.apps.OauthbearerOIDCApp --port %d" \
               % self.conf['port']

    def operational(self):
        self.dbg('Checking if %s is operational' % self.get('valid_url'))
        try:
            r = requests.get(self.get('valid_url'))
            if r.status_code == 200:
                return True
            raise Exception('status_code %d' % r.status_code)
        except Exception as e:
            self.dbg('%s check failed: %s' % (self.get('valid_url'), e))
            return False

    def deploy(self):
        pass
