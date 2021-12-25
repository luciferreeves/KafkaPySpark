from cassandra.cluster import Cluster

# Connect to the cluster and keyspace
cluster = Cluster(['127.0.0.1'], port=9042)
session = cluster.connect()
session.set_keyspace('twitter')
session.execute("USE twitter")

# Insert a tweet in cassandra database if it doesn't exist

def insert_tweet(tweet):
    # Check if tweet exists
    query = "SELECT * FROM twitterdata WHERE tweet_id = %s"
    tweet_exists = session.execute(query, (tweet['tweet_id'],))
    if not tweet_exists:
        # Insert tweet
        query = "INSERT INTO twitterdata (tweet_id, created_at, location, screen_name, quote_count, reply_count, retweet_count, favorite_count, tweet) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
        session.execute(query, (tweet['tweet_id'], tweet['created_at'], tweet['location'], tweet['screen_name'], tweet['quote_count'], tweet['reply_count'], tweet['retweet_count'], tweet['favorite_count'], tweet['tweet']))
        print('Tweet inserted with tweet_id: ' + str(tweet['tweet_id']))
    else:
        print('Tweet already exists with tweet_id: ' + str(tweet['tweet_id']))

##
# insert_user = session.prepare('INSERT INTO table_name (id,name) VALUES (?, ?)')
# batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
# for i,j in some_value:
#     try:
#       batch.add(insert_user,(i,j))
#       logger.info('Data Inserted into the table')
#     except Exception as e:
#       logger.error('The cassandra error: {}'.format(e))
# session.execute(batch)