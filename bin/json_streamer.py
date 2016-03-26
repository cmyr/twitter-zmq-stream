# coding: utf-8
from __future__ import print_function
from __future__ import unicode_literals

from cmpyr import io_utils
from zmqstream.publisher import (StreamPublisher, StreamResult,
                                 StreamResultError, StreamResultItem)

import functools
import time


def make_delay_iter(an_iter, delay=0.01, request_kwargs={}):
    for item in an_iter:
        if item['lang'] == 'en':
            yield StreamResult(StreamResultItem, item)
            time.sleep(delay)


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Does something.")
    parser.add_argument('source_dir', type=str, help="location of (folders of) json files")
    parser.add_argument('--test', action='store_true', help='test json loading, printing to stdout')
    args = parser.parse_args()

    dir_items = io_utils.iter_dir(args.source_dir)
    an_iter = functools.partial(make_delay_iter, dir_items)

    if args.test:
        for item in an_iter():
            print(item)
    else:
        pub = StreamPublisher(
            iterator=an_iter)

        pub.run()


if __name__ == "__main__":
    main()
