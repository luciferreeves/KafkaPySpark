# Live Tweet Sentiment Analysis using Kafka and PySpark
## Zookeeper
```
bin/zookeeper-server-start.sh config/zookeeper.properties
```
## Kafka Server
```
bin/kafka-server-start.sh config/server.properties
```
## Kafka Topic creation
```
bin/kafka-topics.sh --create --partitions 2 --replication-factor 1 --topic twitterdata --bootstrap-server localhost:9092
```

## Goals

- [x] Create a Streaming application that reads from a Kafka topic and writes to a Kafka topic.
- [x] Read tweets from the Kafka topic and write to a Cassandra table.
- [ ] Run a Sentiment Analysis on the fetched static data from the Cassandra database. Use Spacy library.
- [ ] Run the Sentiment Analysis on the dynamically fetched data from the Kafka streaming tweets and delete the data from the Kafka topic as soon as the data is consumed and written to the Cassandra database.
- [ ] Extend this system to multiple producers and consumers.
- [ ] Try extending this to PubSub model.
