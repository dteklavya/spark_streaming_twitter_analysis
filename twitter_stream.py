import twitter
import time
import socket
import sys
import json

from config import *


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
        tweet_text = tweet['text']
        print("Tweet Text: " + tweet_text)
        print ("------------------------------------------")
        tcp_conn.send(bytes(tweet_text + '\n', 'utf-8'))
        time.sleep(1)


TCP_IP = "localhost"
TCP_PORT = 9009
conn = None
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((TCP_IP, TCP_PORT))
s.listen(1)
print("Waiting for TCP connection...")
conn, addr = s.accept()
print("Connected... Starting getting tweets.")
q = 'FM Shri'
stream_tweets_to_spark(q, conn)

