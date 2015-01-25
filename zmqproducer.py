from __future__ import print_function

import time
from random import choice
from random import randrange

import multiprocessing
import zmq
import json
from twitterstream import basic_twitter_stream_iter


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

    def run():
        while True:
            self.process = multiprocessing.Process(
                target=self.start_publishing,
                args=(self.queue,))

    def start_publishing(port):
        context = zmq.context()
        socket = context.socket(zmq.PUB)
        socket.bind("tcp://localhost:%s" % port)
        for line in basic_twitter_stream_iter():
            if line:
                try:
                    tweet = json.loads(line)
                    if tweet.get('text'):
                        msg = str(tweet.get('text'))
                        socket.send(msg)
                except ValueError:
                    continue


if __name__ == "__main__":

    context = zmq.Context()
    socket = context.socket(zmq.PUB)
    socket.bind("tcp://localhost:4999")

    for line in basic_twitter_stream_iter():
        if line:
            try:
                tweet = json.loads(line)
                if tweet.get('text'):
                    msg = str(tweet.get('text'))
                    socket.send(msg)
            except ValueError:
                continue


# def main():
# import argparse
# parser = argparse.ArgumentParser()
# parser.add_argument('arg1', type=str, help="required argument")
# parser.add_argument('arg2', '--argument-2', help='optional boolean argument', action="store_true")
# args = parser.parse_args()


# if __name__ == "__main__":
# main()
