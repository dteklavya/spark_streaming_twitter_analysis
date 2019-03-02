import twitter
import time
import socket
import sys
import json
from langdetect import detect

from config import *
import argparse


def get_twitter_api():
    oauth_token, oauth_token_secret = OAUTH_TOKEN, OAUTH_TOKEN_SECRET

    auth = twitter.oauth.OAuth(oauth_token, oauth_token_secret,
                                   CONSUMER_KEY, CONSUMER_SECRET)

    return twitter.Twitter(auth=auth)


# Get Twitter Streaming OAuth API
def oauth_twitter_streaming():
    oauth_token, oauth_token_secret = OAUTH_TOKEN, OAUTH_TOKEN_SECRET

    auth = twitter.oauth.OAuth(oauth_token, oauth_token_secret,
                                   CONSUMER_KEY, CONSUMER_SECRET)

    return twitter.TwitterStream(auth=auth)


# Generator method. Return tweet text as they are posted/become available
def stream_tweets_to_spark(q, tcp_conn):

    twitter_stream = oauth_twitter_streaming()

    stream = twitter_stream.statuses.filter(track=q)

    for tweet in stream:
#         yield tweet['text']
        print("LANG:", tweet['lang'])
        tweet_text = tweet['text']
        if detect(tweet_text) != 'en':
            continue
        print("Tweet Text: " + tweet_text)
        print ("------------------------------------------")
        tcp_conn.send(bytes(tweet_text + '\n', 'utf-8'))
        time.sleep(1)


def get_tweet_entities(q):
    twitter_stream = oauth_twitter_streaming()

    stream = twitter_stream.statuses.filter(track=q)

    for tweet in stream:
        if 'retweeted_status' in tweet and tweet['retweeted_status']:
#             print("FOUND RETWEETS")
#             rts = tweet['retweeted_status']
#             print(rts['user']['screen_name'], rts['retweet_count'], "BY", tweet['user']['screen_name'])
            # print(rts['text'])
            continue
        if detect(tweet['text']) != 'en':
            continue

        # To check if a user is a bot using the tweet count, do the following-
        # Get tweet['created_at'] (tweet creation time) and get tweet['user']['created_at'] (user creation time.
        # Count the number of days between them.
        # Get tweet['statuses_count'] (number of tweets till date).
        # Divide number of days / statuses_count and
        # It is most probably a bot if the count exceeds 50-75 tweets per day
        # It is surely a bot if count exceeds 144 per day.

        # Find target user who retweet/like trending tweets from prominent ppl.
        print(tweet['user']['screen_name'], tweet['text'])


def get_tweet_by_id(api, id):
    return api.statuses.show(id=id)


def get_tweets_for_hashtag(api, q):
    return api.search.tweets(q=q)


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('q', default="")

    parser.add_argument('runmode', default="hashtag")

    parser.add_argument('id', nargs='?')

    args = parser.parse_args()

    api = get_twitter_api()

    if args.runmode == 'hashtag':
        print("Getting tweets for hashtag", args.q)
        get_tweets_for_hashtag(api, args.q)

    if args.runmode == 'id':
        print("Getting tweet by ID", args.id)
        t = get_tweet_by_id(api, args.id)
        print(t['text'])

    if args.runmode == 'spark':
        print("Streaming tweets to Spark!")
        TCP_IP = "localhost"
        TCP_PORT = 9009
        conn = None
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind((TCP_IP, TCP_PORT))
        s.listen(1)

        print("Waiting for TCP connection...")
        conn, addr = s.accept()
        print("Connected... Starting getting tweets.")
        stream_tweets_to_spark(args.q, conn)

    if args.runmode == 'twentities':
        print("Getting streamed tweet with entities.")
        get_tweet_entities(args.q)
