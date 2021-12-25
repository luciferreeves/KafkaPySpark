from nltk.sentiment.vader import SentimentIntensityAnalyzer
from cassandra.cluster import Cluster
import re
import pandas as pd


compound = []
pos = []
neu = []
neg = []

def sentence_score(rs):
    review_score = SentimentIntensityAnalyzer()
    return review_score.polarity_scores(rs)['compound']
    # compound.append(review_score.polarity_scores(rs)['compound'])
    # neg.append(review_score.polarity_scores(rs)['neg'])
    # neu.append(review_score.polarity_scores(rs)['neu'])
    # pos.append(review_score.polarity_scores(rs)['pos'])


cluster = Cluster(['127.0.0.1'], port=9042)
session = cluster.connect()
session.set_keyspace('twitter')
session.execute("USE twitter")

# Select all tweets from cassandra database
query = "SELECT * FROM twitterdata"
rows = session.execute(query)
tweets = []

# Iterate through all tweets
for row in rows:
    try:
        tweets.append({
            'tweet_id': row.tweet_id,
            'tweet': row.tweet,
            'score': sentence_score(row.tweet)
        })
    except:
        print(row.tweet)


for tweet in tweets:
    if tweet.get('score') > 0.5:
        tweet['sentiment'] = 'positive'
    elif tweet.get('score') < -0.5:
        tweet['sentiment'] = 'negative'
    else:
        tweet['sentiment'] = 'neutral'

df = pd.DataFrame(tweets)
df.to_csv('tweets.csv', index=False)

