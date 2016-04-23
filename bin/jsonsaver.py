# coding: utf-8
from __future__ import print_function
from __future__ import unicode_literals

from zmqstream.consumer import zmq_iter
from cmpyr import io_utils, utils


def stripped_tweet(tweet):
    dict_template = {"text": True, "id_str": True, 'lang': True,
                     "user": {"screen_name": True, "name": True}}
    return utils.prune_dict(tweet, dict_template)


def main():
    '''connects to a stream and saves its contents as gzip'd json'''
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        'outdir', help='base directory for saving files')
    parser.add_argument(
        '-n', '--hostname', type=str, default="localhost",
        help="publisher hostname")
    parser.add_argument(
        '-p', '--port', type=int, default=8069, help="publisher port")
    parser.add_argument(
        '--prune', action='store_true', help='prune json')
    parser.add_argument(
        '-f', '--fileitems', type=int, default=50000,
        help="items to save per file")

    args = parser.parse_args()

    funcargs = dict()
    if args.hostname:
        funcargs['host'] = args.hostname
    if args.port:
        funcargs['port'] = args.port

    to_save = list()
    for msg in zmq_iter(**funcargs):
        if args.prune:
            msg = stripped_tweet(msg)
        to_save.append(msg)
        if len(to_save) >= args.fileitems:
            savepath = io_utils.dump(to_save, args.outdir)
            utils.timeprint('saved %d items to %s' % (len(to_save), savepath))
            to_save = list()


if __name__ == "__main__":
    main()
