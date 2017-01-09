#!/bin/bash
# Some parameters
# Set the root directory. This is where homebrew install your packages (If you have a different directory please manually update this)
root="/usr/local/Cellar/"
kafka_version=0.10.1.0 # you might need to modify this to the version on your computer 
kafka_yahoo_input_topic=quotes
kafka_final_data_topic=trimmedData
spark_version=2.0.1 # you might need to modify this to the version on your computer 
cassandra_version=3.7

cd
cd $root
# Start Kafka
# https://kafka.apache.org/quickstart
# a quick-and-dirty single-node ZooKeeper instance.
$root/kafka/$kafka_version/libexec/bin/zookeeper-server-start.sh $root/kafka/$kafka_version/libexec/config/zookeeper.properties &
# start the Kafka server:
$root/kafka/$kafka_version/libexec/bin/kafka-server-start.sh $root/kafka/$kafka_version/libexec/config/server.properties &
# create a topic 
$root/kafka/$kafka_version/libexec/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic $kafka_yahoo_input_topic &
$root/kafka/$kafka_version/libexec/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic $kafka_final_data_topic &

# Start Apache Spark
# Start up the master node 
$root/apache-spark/$spark_version/libexec/sbin/start-all.sh &
# Start up the slave node
$root/apache-spark/$spark_version/libexec/sbin/start-slaves.sh &

# Start Apache Cassandra
$root/cassandra/$cassandra_version/libexec/bin/cassandra


# Start Apache Cassandra
# spark-submit --master spark://192.168.1.69:7077 --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.1 ./spark_streaming.py
# spark-submit --master spark://ucbvpn-209-166.vpn.berkeley.edu:7077 --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.1 ./spark_streaming.py