# coding: utf-8
from __future__ import print_function
from __future__ import unicode_literals

import sys
import multiprocessing
import zmq
import json
import time
from twitterstream import twitter_stream_iter

# exceptions:
from urllib2 import HTTPError
from socket import error as SocketError

STREAM_TIMEOUT = 90
TWITTER_HTTP_MAX_BACKOFF = 320

class IterPublisher(object):

    """
    IterPublisher takes an iter and publishes it using ØMQ.
    this is used for locally multicasting resources streamed from the WAN
    (for instance)
    """

    def __init__(self, port=8069):
        super(IterPublisher, self).__init__()
        self.port = port
        self.process = None
        self.backoff = None
        self.errors = multiprocessing.Queue()

    def run(self):
        while True:
            if self.process == None:
                self.process = multiprocessing.Process(
                    target=self.start_publishing,
                    args=(self.port, self.errors))
                self.process.daemon = True
                self.process.start()
            self.monitor()
            self.process.terminate()
            self.process = None


    def start_publishing(self, port, error_queue):
        print("starting twitter connection on port %s" % port)

        try:
            stream_session = twitter_stream_iter()
        except HTTPError as err:
            error_queue.put(err.code)
        except SocketServer as err:
            error_queue.put(dict(err))
        finally:
            return

        context = zmq.Context()
        socket = context.socket(zmq.PUB)
        socket.set_hwm(100)
        socket.bind("tcp://127.0.0.1:%s" % port)

        for line in basic_twitter_stream_iter():
            if line:
                try:
                    tweet = json.loads(line)
                    if tweet.get('text'):
                        msg = str(tweet.get('text'))
                        socket.send_string(msg)
                except ValueError:
                    continue

    def monitor(self):
        context = zmq.Context()
        socket = context.socket(zmq.SUB)
        socket.setsockopt_string(zmq.SUBSCRIBE, '')
        socket.connect("tcp://localhost:%s" % self.port)
        last_result = time.time()
        result = ""
        while True:
            time.sleep(0.01)
            try:
                error = self.errors.get_nowait()
                print("stream error: %s" % str(error))
                if error in [400, 401, 403, 404, 405, 406, 407, 408, 410]:
                    self.backoff_for_http()
                if error == 420 or error.get('code') == 420:
                    self.backoff = TWITTER_HTTP_MAX_BACKOFF
                else:
                    continue
            except Queue.Empty:
                continue
            try:
                result = socket.recv_string(flags=zmq.NOBLOCK)
                last_result = time.time()
                self.backoff = None
            except zmq.ZMQError as err:
                if time.time() - last_result > STREAM_TIMEOUT:
                    print("twitter connection timed out")
                    socket.close()
                    return

            sys.stdout.write("\rlast_result: %s at %s" % (result, str(last_result)))
            sys.stdout.flush()

    def backoff_for_http(self):
        if self.backoff == None:
            self.backoff = 5
        else:
            self.backoff = min(self.backoff * 2, TWITTER_HTTP_MAX_BACKOFF)


    

if __name__ == "__main__":
    publisher = IterPublisher()
    publisher.run()


# def main():
# import argparse
# parser = argparse.ArgumentParser()
# parser.add_argument('arg1', type=str, help="required argument")
# parser.add_argument('arg2', '--argument-2', help='optional boolean argument', action="store_true")
# args = parser.parse_args()


# if __name__ == "__main__":
# main()
