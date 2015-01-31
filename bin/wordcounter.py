# coding: utf-8
from __future__ import print_function
from __future__ import unicode_literals

import zmqstream
from collections import defaultdict
import os

BASE_DIR = os.path.expanduser("~/twitter_data/word_counts")


class WordCounter(object):

    """WordCounter counts words"""

    def __init__(self):
        super(WordCounter, self).__init__()
        self.counts = defaultdict(int)
        self.day = time.strftime("%d")

    def add(self, word):
        self.counts[word] += 1

    def add_all(self, sequence):
        for word in sequence:
            self.add(word)

    def run(self, host="localhost", port=8069):
        results = list()

        for item in zmqstream.consumer.zmq_iter(host, port):
            try:
                item = self.filter_item(item)
                if item:
                    self.save_if_needed()
                    words = item.get("text").split()
                    for word in self.filter_words(words):
                        self.add(word)
            except KeyboardInterrupt:
                break
        self.debug_print()

    def save_if_needed(self):
        if self.day != time.strftime("%d"):
            dump(self.counts)
            self.counts = defaultdict(int)
            self.day = time.strftime("%d")

    def debug_print(self):
        to_print = sorted(self.counts.items(),
                          key=lambda tup: tup[1], reverse=True)
        for word, count in to_print:
            print(word.ljust(10), count)


    def filter_item(self, item):
        if item.get("lang") != "en":
            return None
        if item.get('retweeted_status'):
            return None
        return item

    def filter_words(self, word_list):
        word_list = [w.lower() for w in word_list if text_decodes_to_ascii(w)]
        # we include words if the first and last characters are alpha?
        word_list = [w for w in word_list if w[0].isalpha() and w[-1:].isalpha()]
        word_list = [w for w in word_list if not w.startswith("http://")]
        return word_list


def text_decodes_to_ascii(text):
    try:
        text.decode('ascii')
    except UnicodeEncodeError:
        return False
    return True


def dump(results):
    writeDir = os.path.join(BASE_DIR,
                            time.strftime("%Y"),
                            time.strftime("%m"))

    if not os.path.exists(writeDir):
        os.makedirs(writeDir)
    filename = time.strftime("%d.txt.gz")
    filepath = os.path.join(writeDir, filename)
    with gzip.open(filepath, 'wb') as outFile:
        outFile.write(json.dumps(results))


def main():
    word_counter = WordCounter()
    word_counter.run()
    # import argparse
    # parser = argparse.ArgumentParser()
    # parser.add_argument('arg1', type=str, help="required argument")
    # parser.add_argument('arg2', '--argument-2',
    #                     help='optional boolean argument', action="store_true")
    # args = parser.parse_args()


if __name__ == "__main__":
    main()
