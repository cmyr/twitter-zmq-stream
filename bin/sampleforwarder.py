# coding: utf-8
from __future__ import print_function
from __future__ import unicode_literals

import functools

import zmqstream
from zmqstream.publisher import (StreamPublisher, StreamResult,
                                 StreamResultError, StreamResultItem)
import twittertools
import poetryutils2 as poetry


""" cleans up stream results and rebroadcasts them """

def tweet_filter(source_iter):
    for item in source_iter:
        if item.get('retweeted_status'):
            continue
        if item.get('lang') != 'en':
            continue
        if item.get('text'):
            yield stripped_tweet(item)


def stripped_tweet(tweet):
    dict_template = {"text": True, "id_str": True,
                     "user": {"screen_name": True, "name": True}}
    return twittertools.prune_dict(tweet, dict_template)

def line_iter(host="127.0.0.1", port="8069", request_kwargs=None):
    stream = tweet_filter(zmqstream.zmq_iter(host=host, port=port))

    line_filters = [
        poetry.filters.numeral_filter,
        poetry.filters.ascii_filter,
        poetry.filters.url_filter,
        poetry.filters.real_word_ratio_filter(0.9)
    ]

    for line in poetry.line_iter(stream, line_filters, key='text'):
        yield StreamResult(StreamResultItem, line)




def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--hostin', type=str, help="source host")
    parser.add_argument('--portin', type=str, help="source port")
    parser.add_argument('--hostout', type=str, help="host out")
    parser.add_argument('--portout', type=str, help="port out")
    args = parser.parse_args()


    source_host = args.hostin or '127.0.0.1'
    source_port = args.portin or 8069
    dest_host = args.hostout or '127.0.0.1'
    dest_port = args.portout or 8070

    iterator = functools.partial(line_iter, source_host, source_port)

    publisher = StreamPublisher(
        iterator=iterator,
        hostname=dest_host,
        port=dest_port)
    publisher.run()


if __name__ == "__main__":
    main()