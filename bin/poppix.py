# coding: utf-8
from __future__ import print_function
from __future__ import unicode_literals


from collections import defaultdict
from operator import itemgetter
import time

import zmqstream


class PopPix(object):
    """docstring for PopPix"""
    def __init__(self):
        super(PopPix, self).__init__()
        self.activity_indicator = zmqstream.ActivityIndicator()
        self.seen = defaultdict(int)
        self.interest_threshold = 10000
        self.start_time = time.time()
        self.check_interval = 60 * 5  # seconds

    def run(self, host="localhost", port=8069):
        for item in zmqstream.consumer.zmq_iter(host, port):
            orig = item.get('retweeted_status')
            if orig:    
                if orig.get('entities').get('media'):
                    media = orig.get('entities').get('media')
                    rts = orig.get('retweet_count')
                    if media and rts > self.interest_threshold:
                        media_url = media[0].get('media_url')
                        self.seen[media_url] += 1
                else:
                    self.activity_indicator.tick()
                # check our time
                if time.time() - self.start_time > self.check_interval:
                    self.start_time = time.time()
                    most_pop = reduce(most_popular, self.seen.items())
                    print("most pop: %s %d" % (most_pop[0], most_pop[1]))
                    self.seen = defaultdict(int)


def most_popular(item1, item2):
    if item1[1] > item2[1]:
        return item1
    else:
        return item2


                




def main():
    try:
        import setproctitle
        setproctitle.setproctitle("poppix.py")
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

    pp = PopPix()
    pp.run(**funcargs)


if __name__ == "__main__":
    main()