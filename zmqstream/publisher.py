# coding: utf-8
from __future__ import print_function
from __future__ import unicode_literals

import sys
import multiprocessing

try:
    import queue
except ImportError:
    import Queue as queue
import zmq
import json
import time
from collections import namedtuple

from .util import ActivityIndicator

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
                 timeout=90, hostname="127.0.0.1", port=8069):
        super(StreamPublisher, self).__init__()
        self.activity_indicator = ActivityIndicator(
            message="publisher running at %s:%d:" % (hostname, port))
        self.iterator = iterator
        self.iter_kwargs = iter_kwargs
        self.hostname = hostname
        self.port = port
        self.process = None
        self.errors = None
        self.timeout = timeout
        self.backoff_time = 0

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
        socket = context.socket(zmq.PUB)
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
