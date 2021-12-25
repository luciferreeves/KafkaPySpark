from kafka import KafkaConsumer
import json
import re

topic_name = 'twitterdata'

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
    auto_commit_interval_ms =  5000,
    fetch_max_bytes = 128,
    max_poll_records = 100,
    value_deserializer=lambda x: json.loads(x.decode('utf-8')))

counter = 0
for msg in consumer:
    tweet = {}
    message = json.loads(json.dumps(msg.value))
    if 'created_at' in message:
        tweet['created_at'] = message['created_at']
    if 'id' in message:
        tweet['tweet_id'] = message['id']
    if 'location' in message:
        tweet['location'] = message['user']['location']
    if 'screen_name' in message:
        tweet['screen_name'] = message['user']['screen_name']
    if 'quote_count' in message:
        tweet['quote_count'] = message['quote_count']
    if 'reply_count' in message:
        tweet['reply_count'] = message['reply_count']
    if 'retweet_count' in message:
        tweet['retweet_count'] = message['retweet_count']
    if 'favorite_count' in message:
        tweet['favorite_count'] = message['favorite_count']
    if 'text' in message and 'RT @' not in message['text']:
        if ('extended_tweet' in message) and 'full_text' in message['extended_tweet']:
            tweet['tweet'] = deEmojify(message['extended_tweet']['full_text'].replace('\n', ' '))
        else:
            tweet['tweet'] = deEmojify(message['text'].replace('\n', ' '))
    # if 'tweet' in tweet and tweet['tweet'] != '':
    print(counter)
    counter += 1
    if counter == 99:
        break

# End the consumer
consumer.close()
print('Done')