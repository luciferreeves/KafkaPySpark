from kafka import KafkaConsumer
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import math
import string
import random

topic_name = 'twitterdata'

consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='latest',
    enable_auto_commit=True,
    auto_commit_interval_ms =  5000,
    fetch_max_bytes = 128,
    max_poll_records = 100,

    value_deserializer=lambda x: json.loads(x.decode('utf-8')))

if __name__ == '__main__':
    spark = SparkSession.builder.appName("TwitterStreaming").getOrCreate().readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", topic_name).load()
    spark.createOrReplaceTempView("tweets")
    spark.sql("select * from tweets").show()
    spark.stop()
    sc = SparkContext(appName="TwitterStreaming")
    ssc = StreamingContext(sc, 5)
    ssc.checkpoint("checkpoint")
    lines = ssc.socketTextStream("localhost", 9999)
    words = lines.flatMap(lambda line: line.split(" "))
    pairs = words.map(lambda word: (word, 1))
    wordCounts = pairs.reduceByKey(lambda x, y: x + y)
    wordCounts.pprint()
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
    sc.stop()
    consumer.close()
    print("Done")

    

