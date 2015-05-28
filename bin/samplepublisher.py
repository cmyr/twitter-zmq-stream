# coding: utf-8
from __future__ import print_function
from __future__ import unicode_literals

import zmqstream
import requests
import json
from requests_oauthlib import OAuth1
from requests.exceptions import ChunkedEncodingError
from zmqstream.publisher import (StreamPublisher, StreamResult,
                                 StreamResultError, StreamResultItem)
from zmqstream.twittercreds import (CONSUMER_KEY, CONSUMER_SECRET,
                              ACCESS_KEY, ACCESS_SECRET)


class TwitterSampleStream(object):

    """
    very basic single-purpose object for connecting to the streaming API
    in most use-cases python-twitter-tools or tweepy would be preferred
    BUT we need both gzip compression and the 'language' parameter
    """

    def __init__(self,
                 access_key,
                 access_secret,
                 consumer_key,
                 consumer_secret):
        self._access_key = access_key
        self._access_secret = access_secret
        self._consumer_key = consumer_key
        self._consumer_secret = consumer_secret

    def stream_iter(self,
                    endpoint='sample',
                    languages=None,
                    stall_warnings=True):
        auth = OAuth1(self._access_key, self._access_secret,
                      self._consumer_key, self._consumer_secret)

        url = 'https://stream.twitter.com/1.1/statuses/%s.json' % endpoint
        query_headers = {'Accept-Encoding': 'deflate, gzip',
                         'User-Agent': 'cmyr-twitter-tools v0.5'}
        query_params = dict()
        lang_string = None
        if languages:
            if type(languages) is list:
                lang_string = ','.join(languages)
            elif isinstance(languages, basestring):
                lang_string = languages

        if lang_string:
            query_params['language'] = lang_string
        if stall_warnings:
            query_params['stall_warnings'] = True

        stream_connection = requests.get(url,
                                         auth=auth,
                                         stream=True,
                                         params=query_params,
                                         headers=query_headers)
        return stream_connection.iter_lines()


def sample_stream_iter(languages=None):
    stream = TwitterSampleStream(CONSUMER_KEY, CONSUMER_SECRET,
                                         ACCESS_KEY, ACCESS_SECRET)

    stream_connection = stream.stream_iter(languages=languages)
    while True:
        try:
            for line in stream_connection:
                if line:
                    try:
                        tweet = json.loads(line)
                        if tweet.get('delete'):
                            continue
                        if tweet.get('warning') or tweet.get('disconnect'):
                            yield StreamResult(StreamResultError, tweet)
                        elif tweet.get('text'):
                            yield StreamResult(StreamResultItem, tweet)
                        else:
                            print('unknown item:', tweet)
                    except ValueError:
                        continue
        except ChunkedEncodingError as err:
            continue
        except KeyboardInterrupt:
            return

TWITTER_HTTP_MAX_BACKOFF = 320


def twitter_error_handler(error, current_backoff):
    if error in [400, 401, 403, 404, 405, 406, 407, 408, 410]:
        if not current_backoff:
            return 5
        else:
            return min(current_backoff * 2, TWITTER_HTTP_MAX_BACKOFF)
    elif error == 420 or error.get('code') == 420:
        print("backing off for error 420 ø_ø")
        return TWITTER_HTTP_MAX_BACKOFF


def twitter_publisher():
    publisher = StreamPublisher(
        iterator=sample_stream_iter, 
        iter_kwargs={'languages': 'en'}, 
        error_handler=twitter_error_handler)
    publisher.run()


def test():
    for line in sample_stream_iter('en'):
        if line.result_type == StreamResultItem:
            text = line.value.get('text')
            print(text or line or "no line?")
        else:
            print(line)

def main():
    try:
        import setproctitle
        setproctitle.setproctitle('zmq-publisher')
    except ImportError:
        print("missing module: setproctitle")

    # import argparse
    # parser = argparse.ArgumentParser()
    # parser.add_argument(
    #     '-n', '--hostname', type=str, help="publisher hostname")
    # parser.add_argument('-p', '--port', type=str, help="publisher port")
    # parser.add_argument('--langs', type=str, nargs='*',
    #                     help="language codes to narrow scope of twitter stream")
    # args = parser.parse_args()

    # funcargs = dict()
    # if args.hostname:
    #     funcargs['hostname'] = args.hostname
    # if args.port:
    #     funcargs['port'] = args.port
    # if args.langs:
    #     funcargs['langs'] = args.langs

    # publisher = TwitterStreamPublisher(**funcargs)
    # publisher.run()
    return twitter_publisher()



if __name__ == "__main__":
    main()
