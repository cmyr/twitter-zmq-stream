# coding: utf-8
from __future__ import print_function
from __future__ import unicode_literals

import zmqstream
import poetryutils2
from collections import defaultdict
import json
import time
import sys
import os
import gzip

ITEMS_PER_FILE = 20000
BASE_DIR = os.path.expanduser("~/twitter_data")

def run(host="localhost", port=8069):
    results = list()
    seen = 0
    saved = 0
    try:
        for item in zmqstream.consumer.zmq_iter(host, port):
            seen += 1
            if item.get('retweeted_status'):
                # skip retweets
                continue
            if poetryutils2.filters.emoji_filter(item.get('text')):
                results.append({
                    'lang': item.get('lang'),
                    'text': item.get('text')
                    })
                saved += 1
                if len(results) >= ITEMS_PER_FILE:
                    dump(results)
                    results = list()

            sys.stdout.write('\rseen %d, saved %d (%.2f%%)' % (seen, saved, float(saved)/seen))
            sys.stdout.flush()
    except KeyboardInterrupt:
        dump(results)
        return

def dump(results):
    writeDir = os.path.join(BASE_DIR, 
        time.strftime("%Y"),
        time.strftime("%m"),
        time.strftime("%d"))

    if not os.path.exists(writeDir):
        os.makedirs(writeDir)
    filename = time.strftime("%H:%M:%S.txt.gz")
    filepath = os.path.join(writeDir, filename)
    with gzip.open(filepath, 'wb') as outFile:
        outFile.write(json.dumps(results))

    print('\nwrote %d items to %s' % (len(results), filename))


def load():
    files = os.listdir(BASE_DIR)
    for f in files:
        path = os.path.join(BASE_DIR, f)
        items = json.loads(gzip.open(path, 'rb').read())
        for i in items:
            print(i.get('text'))

def main():
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
    run(**funcargs)


if __name__ == "__main__":
    main()