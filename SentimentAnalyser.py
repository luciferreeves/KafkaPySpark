from nltk.sentiment.vader import SentimentIntensityAnalyzer
from cassandra.cluster import Cluster
import re


compound = []
pos = []
neu = []
neg = []

def sentence_score(rs):
    review_score = SentimentIntensityAnalyzer()
    compound.append(review_score.polarity_scores(rs)['compound'])
    neg.append(review_score.polarity_scores(rs)['neg'])
    neu.append(review_score.polarity_scores(rs)['neu'])
    pos.append(review_score.polarity_scores(rs)['pos'])


cluster = Cluster()
session = cluster.connect('twitter')
rows = session.execute('SELECT tweet FROM twitterdata')
for tweet in rows:
    try:
        sentence_score(tweet)
    except re.error:
        print(tweet)