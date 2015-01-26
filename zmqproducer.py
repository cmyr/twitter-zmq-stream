# coding: utf-8
from __future__ import print_function
from __future__ import unicode_literals

import sys
import multiprocessing
import zmq
import json
import time
from twitterstream import basic_twitter_stream_iter


STREAM_TIMEOUT = 60 * 2

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

    def run(self):
        while True:
            if self.process == None:
                self.process = multiprocessing.Process(
                    target=self.start_publishing,
                    args=(self.port,))
                self.process.daemon = True
                self.process.start()
            self.monitor()
            self.process.terminate()
            self.process = None


    def start_publishing(self, port):
        print("starting twitter connection on port %s" % port)
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
        while True:
            time.sleep(0.01)
            result = socket.poll(timeout=1000)
            if result == 0:
                if time.time() - last_result > STREAM_TIMEOUT:
                    print("twitter connection timed out")
                    socket.close()
                    return
            else:
                last_result = time.time()
            sys.stdout.write("\rlast_result: %s at %s" % (result, str(last_result)))
            sys.stdout.flush()


if __name__ == "__main__":
    publisher = IterPublisher()
    publisher.run()
    # context = zmq.Context()
    # socket = context.socket(zmq.PUB)
    # socket.bind("tcp://localhost:4999")

    # for line in basic_twitter_stream_iter():
    #     if line:
    #         try:
    #             tweet = json.loads(line)
    #             if tweet.get('text'):
    #                 msg = str(tweet.get('text'))
    #                 socket.send(msg)
    #         except ValueError:
    #             continue


# def main():
# import argparse
# parser = argparse.ArgumentParser()
# parser.add_argument('arg1', type=str, help="required argument")
# parser.add_argument('arg2', '--argument-2', help='optional boolean argument', action="store_true")
# args = parser.parse_args()


# if __name__ == "__main__":
# main()
