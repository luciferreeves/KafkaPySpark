from kafka import KafkaConsumer
import json
import re
# from database_configuration import insert_tweet
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

topic_name = 'twitterdata'

# Initialize an empty dataframe to store the tweet id and sentiment
tweets = pd.DataFrame(columns=['tweet_id', 'sentiment'])


def sentence_score(rs):
    review_score = SentimentIntensityAnalyzer()
    return review_score.polarity_scores(rs)['compound']

def deEmojify(data):
    emoj = re.compile("["
        u"\U0001F600-\U0001F64F"  # emoticons
        u"\U0001F300-\U0001F5FF"  # symbols & pictographs
        u"\U0001F680-\U0001F6FF"  # transport & map symbols
        u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
        u"\U00002500-\U00002BEF"  # chinese char
        u"\U00002702-\U000027B0"
        u"\U00002702-\U000027B0"
        u"\U000024C2-\U0001F251"
        u"\U0001f926-\U0001f937"
        u"\U00010000-\U0010ffff"
        u"\u2640-\u2642" 
        u"\u2600-\u2B55"
        u"\u200d"
        u"\u23cf"
        u"\u23e9"
        u"\u231a"
        u"\ufe0f"  # dingbats
        u"\u3030"
                      "]+", re.UNICODE)
    return re.sub(emoj, '', data)

consumer = KafkaConsumer(
    topic_name,
    auto_offset_reset='latest',
    enable_auto_commit=True,
    auto_commit_interval_ms =  1000,
    fetch_max_bytes = 128,
    max_poll_records = 100,
    value_deserializer=lambda x: json.loads(x.decode('utf-8')))


for msg in consumer:
    tweet = ""
    message = json.loads(json.dumps(msg.value))
    if 'text' in message and 'RT @' not in message['text']:
        if ('extended_tweet' in message) and 'full_text' in message['extended_tweet']:
            tweet= deEmojify(message['extended_tweet']['full_text'].replace('\n', ' '))
        else:
            tweet= deEmojify(message['text'].replace('\n', ' '))
    if tweet != '':
        score = sentence_score(tweet)
        sentiment = 'neutral'
        if score > 0.5:
            sentiment = 'positive'
        elif score < -0.5:
            sentiment = 'negative'
        tweets = tweets.append({'tweet_id': message['id'], 'sentiment': sentiment}, ignore_index=True)
        # Update the already shown plot with the new dataframe
        tweets.groupby('sentiment').count()['tweet_id'].plot.bar()
        plt.show()
        plt.close('all')


# End the consumer
consumer.close()
print('Done')
