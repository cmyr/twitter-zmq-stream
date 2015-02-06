# coding: utf-8
from __future__ import print_function
from __future__ import unicode_literals

import zmqstream
import poetryutils2
import twittertools
from collections import defaultdict
import json
import time
import sys
import os
import gzip

ITEMS_PER_FILE = 100000
BASE_DIR = os.path.expanduser("~/twitter_data")
VERBOSITY = 0


def run(host="localhost", port=8069, file_length=ITEMS_PER_FILE):
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
                    'text': item.get('text'),
                    'tweet_id': item.get('id_str')
                })
                saved += 1
                if len(results) >= file_length:
                    dump(results)
                    results = list()

            sys.stdout.write('\rseen %d, saved %d (%.2f%%)' %
                             (seen, saved, float(saved) / seen))
            sys.stdout.flush()
    except KeyboardInterrupt:
        dump(results)
        return


def dump(results):
    write_dir = os.path.join(BASE_DIR,
                             time.strftime("%Y"),
                             time.strftime("%m"),
                             time.strftime("%d"))

    create_if_not_exists(write_dir)
    filename = time.strftime("%H:%M:%S.txt.gz")
    filepath = os.path.join(write_dir, filename)
    write_json_to_gzip(filepath, results)


def write_json_to_gzip(outfile, pyobj):
    with gzip.open(outfile, 'wb') as writefile:
        writefile.write(json.dumps(pyobj))
    dprint('\nwrote %d items to %s' % (len(pyobj), outfile))


def create_if_not_exists(newdir):
    if not os.path.exists(newdir):
        os.makedirs(newdir)


def load_file(path, item_filter=None):
    if path.endswith('gz'):
        items = json.loads(gzip.open(path, 'rb').read())
        dprint("loaded %d items from %s" % (len(items), path))
        if item_filter:
            return [item_filter(i) for i in items]
        else:
            return items
    else:
        dprint("skipping file %s" % path)


def iter_file(path, item_filter=None):
    items = load_file(path, item_filter) or list()
    for i in items:
        yield i


def recursive_load_dir(path, item_filter=None, skip_dirs=["sorted"]):
    dprint("loading dir %s" % path)
    items = list()
    files = os.listdir(path)
    dprint("loading %s files" % "\n".join(files))
    for f in files:
        if f in skip_dirs:
            dprint("skipping %s" % f)
            continue
        abspath = os.path.join(path, f)
        if os.path.isdir(abspath):
            items.extend(recursive_load_dir(abspath))
        else:
            file_items = load_file(abspath, item_filter)
            if file_items:
                items.extend(file_items)
    return items


def iter_dir(path, item_filter=None, skip_dirs=["sorted"]):
    files = os.listdir(path)
    dprint("iterating %s files" % "\n".join(files))
    for f in files:
        if f in skip_dirs:
            continue
        abspath = os.path.join(path, f)
        if os.path.isdir(abspath):
            for item in iter_dir(abspath, item_filter):
                try:
                    yield item
                except StopIteration:
                    continue
        else:
            for item in iter_file(abspath, item_filter):
                try:
                    yield item
                except StopIteration:
                    continue


def lang_count(items):
    by_lang = defaultdict(list)
    for i in items:
        by_lang[i.get('lang')].append(i.get('text'))
    return by_lang

"""sort of a bastard main() function, at this point. """


def lang_sort(dir_path, print_lang=None, sample=False):
    if not print_lang and not sample:
        return efficient_count(dir_path)
    from operator import itemgetter
    dprint("lang sorting %s" % dir_path)
    all_items = recursive_load_dir(dir_path)
    langs = lang_count(all_items)
    counts = [(l, len(i)) for l, i in langs.items()]
    counts.sort(key=lambda tup: tup[1], reverse=True)

    print("loaded %d total items" % len(all_items))
    for lang, items in counts:
        print("%s: %d" % (lang, items))

    if sample:
        for l, items in langs.items():
            print(items[0])

    if print_lang:
        to_print = langs.get(print_lang)
        if to_print:
            dprint("found %d items to print" % len(to_print))
            for p in to_print:
                print(p)
        else:
            dprint("found no lines to print")


def efficient_count(dir_path):
    from collections import Counter
    dprint("\nefficient counting %s" % dir_path)
    counts = Counter(iter_dir(dir_path, lang_count_filter))
    for item, count in counts.most_common(100):
        lang = twittertools.language_name(item)
        lang = lang + ":"
        print("%s%d" % (lang.ljust(15), count))


def lang_write(dir_path):
    out_dir = "sorted"
    all_items = recursive_load_dir(dir_path, skip_dirs=[out_dir])
    langs = lang_count(all_items)
    write_dir = os.path.join(dir_path, "sorted")
    create_if_not_exists(write_dir)
    for lang, items in langs.items():
        filename = next_numbered_file(write_dir, lang)
        write_json_to_gzip(filename, items)


def next_numbered_file(dir_path, basename):
    number_extension = 0
    while True:
        ext = "%d" % number_extension if number_extension else ""
        nextname = os.path.join(dir_path, "%s%s.txt.gz" % (basename, ext))
        if not os.path.exists(nextname):
            dprint("next filename = %s" % nextname)
            return nextname
        number_extension += 1


def dprint(output):
    if VERBOSITY > 0:
        print(output)


def lang_count_filter(inp):
    return inp.get('lang')


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('-n', '--hostname', type=str, default="localhost",
                        help="publisher hostname")
    parser.add_argument('-p', '--port', type=str, help="publisher port")
    parser.add_argument(
        '--file-length', type=int, help="optional: size of write files")
    parser.add_argument(
        '--lang-sort', type=str, help="sort input directory by language")
    parser.add_argument('--print-lang', type=str,
                        help="optional language code will be printed to stdout")
    parser.add_argument('-v', '--verbose',
                        action="store_true", help="display debug information")
    parser.add_argument('-w', '--write',
                        action="store_true", help="sort and write files to disk by language")
    parser.add_argument('-s', '--sample',
                        action="store_true", help="with --lang-sort, \
                        optionally prints one item from each language")

    args = parser.parse_args()

    funcargs = dict()
    if args.verbose:
        global VERBOSITY
        VERBOSITY = 1

    if args.lang_sort:
        if args.write:
            return lang_write(args.lang_sort)
        return lang_sort(args.lang_sort, args.print_lang, args.sample)

    if args.hostname:
        funcargs['host'] = args.hostname
    if args.port:
        funcargs['port'] = args.port
    if args.file_length:
        funcargs['file_length'] = args.file_length
    run(**funcargs)


if __name__ == "__main__":
    main()
