# coding: utf-8
from __future__ import print_function
from __future__ import unicode_literals

import poetryutils2 as poetry
import zmqstream

def tweet_filter(source_iter):
    for item in source_iter:
        if item.get('retweeted_status'):
            continue
        if item.get('lang') != 'en':
            continue
        if item.get('text'):
            yield item.get('text')


def run(host="127.0.0.1", port="8069"):
    poet = poetry.Limericker()
    tweet_texts = tweet_filter(zmqstream.zmq_iter(host=host, port=port))

    line_filters = [
    poetry.filters.numeral_filter,
    poetry.filters.ascii_filter,
    poetry.filters.url_filter,
    poetry.filters.real_word_ratio_filter(0.9)
    ]

    source = poetry.line_iter(tweet_texts, line_filters)
    for poem in poet.generate_from_source(iter_wrapper(source)):
        print(poet.prettify(poem))


def iter_wrapper(source_iter):
    activity_indicator = zmqstream.ActivityIndicator()
    for i in source_iter:
        last_word = i.split()[-1]
        activity_indicator.message = last_word
        activity_indicator.tick()
        yield i


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
    args = parser.parse_args()

    funcargs = dict()
    if args.hostname:
        funcargs['host'] = args.hostname
    if args.port:
        funcargs['port'] = args.port

    run(**funcargs)

if __name__ == "__main__":
    main()