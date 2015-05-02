# coding: utf-8
from __future__ import print_function
from __future__ import unicode_literals


from collections import defaultdict
from operator import itemgetter
import time

import zmqstream
import re
import os

BASE_DIR = os.path.expanduser("~/twitter_data/monkeys")

class StreamScanner(object):
    def __init__(self):
        super(StreamScanner, self).__init__()
        self.activity_indicator = zmqstream.ActivityIndicator()
        self.regex = r'monkey.*shakespeare'
        self.filepath = os.path.join(BASE_DIR, time.strftime("%b%d-%H-%M-%S.txt"))

    def run(self, host="localhost", port=8069):
        for item in zmqstream.consumer.zmq_iter(host, port):
            text = item.get('text')
            if self.check_text(text):
                # print(text)
                self.write_line(item)
            self.activity_indicator.tick()

    def write_line(self, tweet):
        if not os.path.exists(self.filepath):
            with open(self.filepath, 'w') as f:
                pass

        with open(os.path.join(BASE_DIR, self.filepath), 'a') as f:
            u = tweet.get('user').get('screen_name')
            i = tweet.get('id_str')
            t = tweet.get('text')
            line = " ".join([u + i + t + "\n"])
            print(line)
            f.write(line.encode('utf-8'))


    def check_text(self, text):
        if re.search(self.regex, text, flags=re.I):
            return True


def test():
    strings = [
    "the monkeys have arrived and they want to know where to put the shakespeare down",
    "the monkey wrote the Shakespeare",
    "the money shakes peirs"]

    scanner = StreamScanner()
    results = [t for t in strings if scanner.check_text(t)]
    print(results)


def main():
    # return test()
    try:
        import setproctitle
        setproctitle.setproctitle("the_monkeys.py")
    except ImportError:
        print("missing module: setproctitle")
        pass

    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('-n', '--hostname', type=str, default="localhost",
                        help="publisher hostname")
    parser.add_argument('-p', '--port', type=str, help="publisher port")
    args = parser.parse_args()

    funcargs = dict()
    if args.hostname:
        funcargs['host'] = args.hostname
    if args.port:
        funcargs['port'] = args.port

    monkey = StreamScanner()
    # monkey.regex = "hello"
    monkey.run(**funcargs)


if __name__ == "__main__":
    main()