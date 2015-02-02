# coding: utf-8
from __future__ import print_function
from __future__ import unicode_literals

import sys
import multiprocessing
import Queue
import zmq
import json
import time
from twitterstream import twitter_stream_iter

from util import ActivityIndicator

# exceptions:
from urllib2 import HTTPError
from socket import error as SocketError
from requests.exceptions import ChunkedEncodingError

STREAM_TIMEOUT = 90
TWITTER_HTTP_MAX_BACKOFF = 320


class TwitterStreamPublisher(object):

    """
    TwitterStreamPublisher takes an iter and publishes it using ØMQ.
    this is used for locally multicasting resources streamed from the WAN
    (for instance)
    """

    def __init__(self, hostname="127.0.0.1", port=8069):
        super(TwitterStreamPublisher, self).__init__()
        self.activity_indicator = ActivityIndicator(
            message="publisher running:")
        self.port = port
        self.hostname = hostname
        self.process = None
        self.backoff = None
        self.errors = None

    def run(self):
        while True:
            if self.process == None:
                self.errors = multiprocessing.Queue()
                self.process = multiprocessing.Process(
                    target=self.start_publishing,
                    args=(self.hostname, self.port, self.errors))
                self.process.daemon = True
                self.process.start()
            try:
                self.monitor()
                self.process.terminate()
                self.process = None
            except KeyboardInterrupt as err:
                print("\nclosing stream publisher")
                break

    def start_publishing(self, host, port, error_queue):
        print("publishing stream at %s:%s" % (host, port))

        context = zmq.Context()
        socket = context.socket(zmq.PUB)
        socket.set_hwm(100)
        socket.bind("tcp://%s:%s" % (str(host), str(port)))

        try:
            stream_session = twitter_stream_iter()
        except HTTPError as err:
            error_queue.put(err.code)
            return
        except SocketServer as err:
            error_queue.put(dict(err))
            return
        while True:
            try:
                for line in stream_session:
                    if line:
                        try:
                            tweet = json.loads(line)
                            if tweet.get('warning'):
                                error_queue.put(dict(tweet))
                                continue
                            if tweet.get('disconnect'):
                                error_queue.put(dict(tweet))
                                continue
                            if tweet.get('text'):
                                # after checking contents, publish raw json
                                socket.send_string(line)
                        except ValueError:
                            continue
            except ChunkedEncodingError as err:
                continue
            except KeyboardInterrupt:
                return

    def monitor(self):
        context = zmq.Context()
        socket = context.socket(zmq.SUB)
        socket.setsockopt_string(zmq.SUBSCRIBE, '')
        socket.connect("tcp://%s:%s" % (self.hostname, str(self.port)))
        last_result = time.time()
        result = ""
        while True:
            time.sleep(0.01)
            if self.error():
                print('error break!')
                break
            try:
                result = socket.recv_string(flags=zmq.NOBLOCK)
                last_result = time.time()
                self.backoff = None
            except zmq.ZMQError as err:
                if time.time() - last_result > STREAM_TIMEOUT:
                    print("twitter connection timed out")
                    socket.close()
                    return

            self.activity_indicator.tick()

    def error(self):
        try:
            error = self.errors.get_nowait()
            if error in [400, 401, 403, 404, 405, 406, 407, 408, 410]:
                self.backoff_for_http()
                print("backing off for error %d" % error)
                return error
            if error == 420 or error.get('code') == 420:
                print("backing off for error 420 ø_ø")
                self.backoff = TWITTER_HTTP_MAX_BACKOFF
                return error
            else:
                print_error(erorr)
        except Queue.Empty:
            pass

    def backoff_for_http(self):
        if self.backoff == None:
            self.backoff = 5
        else:
            self.backoff = min(self.backoff * 2, TWITTER_HTTP_MAX_BACKOFF)


def print_error(error):
    warning = error.get('warning')
    if warning:
        print("%s: %s: %s" % (time.ctime(),
                              warning.get("code"),
                              warning.get("message")))
    else:
        print(error)


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-n', '--hostname', type=str, help="publisher hostname")
    parser.add_argument('-p', '--port', type=str, help="publisher port")
    args = parser.parse_args()

    funcargs = dict()
    if args.hostname:
        funcargs['hostname'] = args.hostname
    if args.port:
        funcargs['port'] = args.port

    publisher = TwitterStreamPublisher(**funcargs)
    publisher.run()


if __name__ == "__main__":
    main()
