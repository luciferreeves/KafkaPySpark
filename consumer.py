from kafka import KafkaConsumer
import json
import re
# from database_configuration import insert_tweet
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from time import sleep
from multiprocessing import Process, Queue
import matplotlib.pyplot as plt
import matplotlib.animation as animation

topic_name = 'twitterdata'

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

def consumer(q):
    consumer = KafkaConsumer(
    topic_name,
    auto_offset_reset= 'earliest',
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
            if score > 0:
                sentiment = 'positive'
            elif score < 0:
                sentiment = 'negative'
            q.put(({'tweet_id': message['id'], 'tweet': tweet, 'sentiment': sentiment}))

x_labels = ["Positive", "Negative", "Neutral"]
y_data = [0, 0, 0]

if __name__ == '__main__':
    q = Queue()
    p = Process(target=consumer, args=(q,))
    p.start()

    def update_data():
        # Get new data from the queue
        new_data = q.get()
        # Check the sentiment of the tweet
        if new_data['sentiment'] == 'positive':
            y_data[0] += 1
        elif new_data['sentiment'] == 'negative':
            y_data[1] += 1
        else:
            y_data[2] += 1

    fig = plt.figure()
    plt.barh(x_labels, y_data, color=['green', 'red', 'blue'])

    def animate(i):
        update_data()
        plt.barh(x_labels, y_data, color=['green', 'red', 'blue'])

    ani = animation.FuncAnimation(fig, animate, interval=1, frames=100)
    plt.show()
