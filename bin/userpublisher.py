# coding: utf-8
from __future__ import print_function
from __future__ import unicode_literals

import zmqstream
import requests
import json

from requests.exceptions import ChunkedEncodingError
from twitter.stream import TwitterStream
from twitter.oauth import OAuth

from zmqstream.publisher import (StreamPublisher, StreamResult,
                                 StreamResultError, StreamResultItem)

from zmqstream.twittercreds import (CONSUMER_KEY, CONSUMER_SECRET,
                                    ACCESS_KEY, ACCESS_SECRET,
                                    P_ACCESS_KEY, P_ACCESS_SECRET,
                                    P_CONSUMER_KEY, P_CONSUMER_SECRET)


def twitter_user_stream(request_params):
    # auth = OAuth(P_ACCESS_KEY, P_ACCESS_SECRET,
    #              P_CONSUMER_KEY, P_CONSUMER_SECRET)
    auth = OAuth(ACCESS_KEY, ACCESS_SECRET,
                 CONSUMER_KEY, CONSUMER_SECRET)
    print('starting user stream with params:, ', request_params)
    return TwitterStream(
        auth=auth,
        domain='userstream.twitter.com').user(**request_params)


def user_stream_iter(request_params):
    stream_connection = twitter_user_stream(request_params)
    while True:
        try:
            for tweet in stream_connection:
                if tweet:
                    try:
                        # tweet = json.loads(line)
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
        iterator=user_stream_iter,
        error_handler=twitter_error_handler)
    publisher.run()


def test():
    for line in user_stream_iter({'replies': 'all'}):
        if line.result_type == StreamResultItem:
            text = line.value.get('text')
            print(text or line or "no line?")
        else:
            print(line)


def main():
    # return test()
    try:
        import setproctitle
        setproctitle.setproctitle('zmq-publisher')
    except ImportError:
        print("missing module: setproctitle")

    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-n', '--hostname', type=str, help="publisher hostname")
    parser.add_argument('-p', '--port', type=str, help="publisher port")
    parser.add_argument('-a', '--all-replies',
                        help='include non-follower replies',
                        action="store_true")
    parser.add_argument('-u', '--user-only',
                        help="don't include items from followed accounts",
                        action="store_true")
    args = parser.parse_args()

    funcargs = dict()
    iter_kwargs = dict()
    if args.hostname:
        funcargs['hostname'] = args.hostname
    if args.port:
        funcargs['port'] = args.port
    if args.all_replies:
        iter_kwargs['replies'] = 'all'
    if args.user_only:
        iter_kwargs['with'] = 'user'

    publisher = StreamPublisher(
        iterator=user_stream_iter,
        iter_kwargs=iter_kwargs,
        error_handler=twitter_error_handler,
        **funcargs)

    return publisher.run()


if __name__ == "__main__":
    main()
