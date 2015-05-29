# coding: utf-8
from __future__ import print_function
from __future__ import unicode_literals

import sys
import multiprocessing
import os

try:
    import queue
except ImportError:
    import Queue as queue
import zmq
import zmq.auth
from zmq.auth.thread import ThreadAuthenticator
import json
import time
from collections import namedtuple

from .util import (ActivityIndicator, keys_dirs)

# exceptions:
try:
    from urllib2 import HTTPError
except ImportError:
    pass
from socket import error as SocketError


DEFAULT_BACKOFF_TIME = 120


StreamResult = namedtuple('StreamResult', ['result_type', 'value'])
StreamResultError = 'StreamResultError'
StreamResultItem = 'StreamResultItem'
StreamResultKeepAlive = 'StreamResultKeepAlive'


class StreamPublisher(object):

    """takes an iterator and broadcasts its items using ZMQ"""

    def __init__(self, iterator, iter_kwargs={}, error_handler=None,
                 timeout=90, hostname="127.0.0.1", port=8069,
                 require_auth=False):
        super(StreamPublisher, self).__init__()
        self.activity_indicator = ActivityIndicator(
            message="publisher running at %s:%d; encrypted: %s" %
            (hostname, port, str(require_auth)))
        self.iterator = iterator
        self.iter_kwargs = iter_kwargs
        self.hostname = hostname
        self.port = port
        self.process = None
        self.errors = None
        self.timeout = timeout
        self.backoff_time = 0
        self.require_auth = require_auth

    def run(self):
        while True:
            if self.process == None:
                self.errors = multiprocessing.Queue()
                self.process = multiprocessing.Process(
                    target=self.start_publishing,
                    args=(self.iterator, self.iter_kwargs, self.hostname,
                          self.port, self.errors))
                self.process.daemon = True
                self.process.start()
            try:
                self.monitor()
                self.process.terminate()
                self.process = None
                time.sleep(self.backoff_time)
            except KeyboardInterrupt as err:
                print("\nclosing stream publisher")
                break

        self.process.terminate()
        self.process = None

    def start_publishing(self, iterator, kwargs, host, port, error_queue):
        print("publishing stream at %s:%s" % (host, port))

        try:
            import setproctitle
            setproctitle.setproctitle('zmqproducer')
        except ImportError:
            print("missing module: setproctitle")

        context = zmq.Context()

        if self.require_auth:
            auth = ThreadAuthenticator(context)
            auth.start()
            auth.allow('127.0.0.1')
            auth.configure_curve(domain='*', location=zmq.auth.CURVE_ALLOW_ANY)

        socket = context.socket(zmq.PUB)

        if self.require_auth:
            _, secret_keys_dir = keys_dirs()
            server_secret_file = os.path.join(
                secret_keys_dir, "server.key_secret")
            server_public, server_secret = zmq.auth.load_certificate(
                server_secret_file)
            socket.curve_secretkey = server_secret
            socket.curve_publickey = server_public
            socket.curve_server = True  # must come before bind

        socket.set_hwm(100)
        socket.bind("tcp://%s:%s" % (str(host), str(port)))

        try:
            stream_session = iterator(request_kwargs=kwargs)
        except HTTPError as err:
            error_queue.put(err.code)
            return
        except SocketError as err:
            error_queue.put(dict(err))
            return
        while True:
            try:
                for item in stream_session:
                    try:
                        if item.result_type == StreamResultError:
                            error_queue.put(item.value)
                        elif item.result_type == StreamResultItem:
                            socket.send_string(json.dumps(item.value))
                        elif item.result_type == StreamResultKeepAlive:
                            socket.send_string(json.dumps(
                                {'keep_alive': True}))

                    except ValueError:
                        print('value error with item:', item)
                        continue
            except KeyboardInterrupt:
                break

    def monitor(self):
        context = zmq.Context()
        socket = context.socket(zmq.SUB)
        if self.require_auth:
            public_keys_dir, secret_keys_dir = keys_dirs()
            client_secret_file = os.path.join(
                secret_keys_dir, "client.key_secret")
            client_public, client_secret = zmq.auth.load_certificate(
                client_secret_file)
            socket.curve_secretkey = client_secret
            socket.curve_publickey = client_public

            server_public_file = os.path.join(
                public_keys_dir, "server.key")
            server_public, _ = zmq.auth.load_certificate(server_public_file)
            # The client must know the server's public key to make a CURVE
            # connection.
            socket.curve_serverkey = server_public
        socket.setsockopt_string(zmq.SUBSCRIBE, '')
        socket.connect("tcp://%s:%s" % (self.hostname, str(self.port)))
        last_result = time.time()
        result = ""
        while True:
            time.sleep(0.01)
            if self.handle_errors():
                print('error break!')
                break
            try:
                result = socket.recv_string(flags=zmq.NOBLOCK)
                self.activity_indicator.tick()
                last_result = time.time()
                self.backoff_time = 0
            except zmq.ZMQError as err:
                if time.time() - last_result > self.timeout:
                    print("stream connection timed out")
                    socket.close()
                    return

    def handle_errors(self):
        try:
            error = self.errors.get_nowait()
            if self.error_handler:
                self.backoff_time = self.error_handler(
                    error, self.backoff_time)
            else:
                self.backoff_time = DEFAULT_BACKOFF_TIME
            return error
        except queue.Empty:
            pass


def main():
    pass

if __name__ == "__main__":
    main()
