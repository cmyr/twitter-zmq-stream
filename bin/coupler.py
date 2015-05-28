# coding: utf-8
from __future__ import print_function
from __future__ import unicode_literals

import json
import os
import time

import poetryutils2 as poetry
import twittertools
import zmqstream



BASE_DATA_DIR = os.path.expanduser("~/twitter_data/poems/couplets")
if not os.path.exists(BASE_DATA_DIR):
    os.makedirs(BASE_DATA_DIR)

def tweet_filter(source_iter):
    for item in source_iter:
        if item.get('retweeted_status'):
            continue
        if item.get('lang') != 'en':
            continue
        if item.get('text'):
            yield stripped_tweet(item)


def stripped_tweet(tweet):
    dict_template = {"text": True, "id_str": True, "user": {"screen_name": True, "name": True}}
    return twittertools.prune_dict(tweet, dict_template)


def run(host="127.0.0.1", port="8069", debug=False, save_json=False):
    poet = poetry.Coupler()
    tweet_texts = tweet_filter(zmqstream.zmq_iter(host=host, port=port))
    # tweet_texts = open(os.path.expanduser('~/tweetdbm/may04.txt')).readlines()

    line_filters = [
    poetry.filters.numeral_filter,
    poetry.filters.ascii_filter,
    poetry.filters.url_filter,
    poetry.filters.real_word_ratio_filter(0.9)
    ]

    source = poetry.line_iter(tweet_texts, line_filters)
    for poem in poet.generate_from_source(iter_wrapper(source)):
        print(poet.prettify(poem))
        if save_json:
            save_as_json(poet.dictify(poem))



def iter_wrapper(source_iter, key=None):
    activity_indicator = zmqstream.ActivityIndicator()
    count = 0
    for i in source_iter:
        count += 1
        if not key:
            last_word = i.split()[-1]
        else:
            last_word = i.get(key).split()[-1]
        # activity_indicator.message = last_word + " %d" % count
        # activity_indicator.tick()
        yield i


def save_as_json(poem):
    filename = time.strftime("limerick-%b-%d-%H:%M:%S.json")
    filename = os.path.join(BASE_DATA_DIR, filename)
    with open(filename, "w") as f:
        json.dump(poem, f)


def main():
    try:
        import setproctitle
        setproctitle.setproctitle('limericker')
    except ImportError:
        print("missing module: setproctitle")


    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('-n', '--hostname', type=str, default="localhost",
                        help="publisher hostname")
    parser.add_argument('-p', '--port', type=str, help="publisher port")
    parser.add_argument('-v', '--verbose', help="print debug info", action="store_true")
    parser.add_argument('-j', '--json', help="save json", action="store_true")
    args = parser.parse_args()

    funcargs = dict()
    funcargs['debug'] = args.verbose
    if args.hostname:
        funcargs['host'] = args.hostname
    if args.port:
        funcargs['port'] = args.port
    if args.json:
        funcargs['save_json'] = args.json

    run(**funcargs)

if __name__ == "__main__":
    main()

