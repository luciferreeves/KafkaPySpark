# KafkaPySpark
# Zookeeper
bin/zookeeper-server-start.sh config/zookeeper.properties
# Kafka Server
bin/kafka-server-start.sh config/server.properties
# Kafka Topic creation
bin/kafka-topics.sh --create --partitions 2 --replication-factor 1 --topic twitterdata --bootstrap-server localhost:9092