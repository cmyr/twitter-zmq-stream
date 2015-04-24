# coding: utf-8
from __future__ import print_function
from __future__ import unicode_literals


from collections import defaultdict
from operator import itemgetter
import time

import zmqstream
import re

class MonkeyFinder(object):
    def __init__(self):
        super(MonkeyFinder, self).__init__()
        self.activity_indicator = zmqstream.ActivityIndicator()
        self.regex = r'monkey.*shakespeare'

    def run(self, host="localhost", port=8069):
        for item in zmqstream.consumer.zmq_iter(host, port):
            text = item.get('text')
            if self.check_text(text):
                print(text)
            self.activity_indicator.tick()


    def check_text(self, text):
        if re.search(self.regex, text, flags=re.I):
            return True


def test():
    strings = [
    "the monkeys have arrived and they want to know where to put the shakespeare",
    "the monkey wrote the Shakespeare",
    "the money shakes peirs"]

    scanner = MonkeyFinder()
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

    monkey = MonkeyFinder()
    monkey.run(**funcargs)


if __name__ == "__main__":
    main()