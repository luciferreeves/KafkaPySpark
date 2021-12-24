from dotenv import load_dotenv
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import KafkaProducer
import os

load_dotenv()

access_token = os.environ.get('ACCESS_TOKEN')
access_token_secret = os.environ.get('ACCESS_TOKEN_SECRET')
consumer_key = os.environ.get('CONSUMER_KEY')
consumer_secret = os.environ.get('CONSUMER_SECRET')

producer = KafkaProducer(bootstrap_servers='localhost:9092')

topic_name = 'twitterdata'

class twitterAuth():
    """Set up Twitter Authentication"""
    def authenticateTwitterApp(self):
        auth = OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token, access_token_secret)
        return auth

class TwitterStreamer():
    """Set up Streamer"""
    def __init__(self):
        self.twitterAuth = twitterAuth()

    def stream_tweets(self):
        while True:
            listener = ListenerTS()
            auth = self.twitterAuth.authenticateTwitterApp()
            stream = Stream(auth, listener)
            stream.filter(track=["Apple"], stall_warnings=True, languages= ["en"])

class ListenerTS(StreamListener):

    def on_data(self, raw_data):
            producer.send(topic_name, str.encode(raw_data))
            return True

if __name__ == "__main__":
    TS = TwitterStreamer()
    TS.stream_tweets()