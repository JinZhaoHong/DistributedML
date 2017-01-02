#!/bin/bash
# Some parameters
# Set the root directory. This is where homebrew install your packages (If you have a different directory please manually update this)
root="/usr/local/Cellar/"
kafka_version=0.10.1.0 # you might need to modify this to the version on your computer 
kafka_topic=quotes
spark_version=2.0.1 # you might need to modify this to the version on your computer 
cassandra_version=3.7

cd
cd $root

$root/kafka/$kafka_version/libexec/bin/kafka-server-stop.sh
$root/kafka/$kafka_version/libexec/bin/zookeeper-server-stop.sh

$root/apache-spark/$spark_version/libexec/sbin/stop-all.sh

# Manually find Cassandra's pid to kill it
#ps auwx | grep cassandra
#sudo kill pid



