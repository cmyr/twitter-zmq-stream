# coding: utf-8
from __future__ import print_function
from __future__ import unicode_literals

import zmqstream
import requests
import json
import functools

from requests_oauthlib import OAuth1
from requests.exceptions import ChunkedEncodingError

from twittertools import load_auth
from zmqstream.publisher import (StreamPublisher, StreamResult,
                                 StreamResultError, StreamResultItem)


class TwitterSampleStream(object):

    """
    very basic single-purpose object for connecting to the streaming API
    in most use-cases python-twitter-tools or tweepy would be preferred
    BUT we need both gzip compression and the 'language' parameter
    """

    def __init__(self, auth):
        self.auth = auth

    def stream_iter(self,
                    endpoint='sample',
                    languages=None,
                    stall_warnings=True):

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
                                         auth=self.auth,
                                         stream=True,
                                         params=query_params,
                                         headers=query_headers)
        return stream_connection.iter_lines()


def sample_stream_iter(auth, request_kwargs):
    stream = TwitterSampleStream(auth)

    stream_connection = stream.stream_iter(**request_kwargs)
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
    else: 
        print('handling unexpected error %s' % error)
        return TWITTER_HTTP_MAX_BACKOFF


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

    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument(
        'auth', type=str, help="twitter auth token file")
    parser.add_argument(
        '-n', '--hostname', type=str, help="publisher hostname")
    parser.add_argument('-p', '--port', type=str, help="publisher port")
    parser.add_argument('--langs', type=str, nargs='*',
                        help="only include tweets with these language codes")
    args = parser.parse_args()

    func_kwargs = dict()
    iter_kwargs = dict()
    if args.hostname:
        func_kwargs['hostname'] = args.hostname
    if args.port:
        func_kwargs['port'] = args.port
    if args.langs:
        iter_kwargs['languages'] = args.langs

    credentials = load_auth(args.auth, raw=True)
    auth = OAuth1(*credentials)
    iterator = functools.partial(sample_stream_iter, auth)

    publisher = StreamPublisher(
        iterator=iterator,
        iter_kwargs=iter_kwargs,
        error_handler=twitter_error_handler,
        **func_kwargs)
    publisher.run()


if __name__ == "__main__":
    main()
