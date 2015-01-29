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
VERBOSITY = 0


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

            sys.stdout.write('\rseen %d, saved %d (%.2f%%)' %
                             (seen, saved, float(saved) / seen))
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


def load_file(path):
    if path.endswith('gz'):
        items = json.loads(gzip.open(path, 'rb').read())
        dprint("loaded %d items from %s" % (len(items), path))
        return items
    else:
        dprint("skipping file %s" % path)


def recursive_load_dir(path):
    dprint("loading dir %s" % path)
    items = list()
    files = os.listdir(path)
    dprint("loading %s files" % "\n".join(files))
    for f in files:
        abspath = os.path.join(path, f)
        if os.path.isdir(abspath):
            items.extend(load_dir(abspath))
        else:
            file_items = load_file(abspath)
            if file_items:
                items.extend(file_items)
    return items


def lang_count(items):
    by_lang = defaultdict(list)
    for i in items:
        by_lang[i.get('lang')].append(i.get('text'))
    return by_lang


def lang_sort(dir_path, print_lang=None):
    from operator import itemgetter
    dprint("lang sorting %s" % dir_path)
    all_items = load_dir(dir_path)
    langs = lang_count(all_items)
    counts = [(l, len(i)) for l, i in langs.items()]
    counts.sort(key=lambda tup: tup[1], reverse=True)

    print("loaded %d total items" % len(all_items))
    for lang, items in counts:
        print("%s: %d" % (lang, items))

    if print_lang:
        to_print = langs.get(print_lang)
        if to_print:
            dprint("found %d items to print" % len(to_print))
            for p in to_print:
                print(p)
        else:
            dprint("found no lines to print")


def dprint(output):
    if VERBOSITY > 0:
        print(output)


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('-n', '--hostname', type=str, default="localhost",
                        help="publisher hostname")
    parser.add_argument('-p', '--port', type=str, help="publisher port")
    parser.add_argument(
        '--lang-sort', type=str, help="sort input directory by language")
    parser.add_argument('--print-lang', type=str,
                        help="optional language code will be printed to stdout")
    parser.add_argument('-v', '--verbose',
                        action="store_true", help="display debug information")
    args = parser.parse_args()

    funcargs = dict()
    if args.verbose:
        global VERBOSITY
        VERBOSITY = 1

    if args.lang_sort:
        lang_sort(args.lang_sort, args.print_lang)
        return
    if args.hostname:
        funcargs['host'] = args.hostname
    if args.port:
        funcargs['port'] = args.port
    run(**funcargs)


if __name__ == "__main__":
    main()
