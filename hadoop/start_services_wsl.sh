#!/bin/bash
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export HADOOP_HOME=/home/mitke/hadoop
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

echo "=== Dọn dẹp tiến trình cũ nếu có ==="
pkill -f 'org.apache.hadoop' 2>/dev/null
pkill -f 'kafka.Kafka' 2>/dev/null
pkill -f 'QuorumPeerMain' 2>/dev/null
sleep 2

echo "=== Khởi động Hadoop HDFS NameNode và DataNode ==="
setsid $HADOOP_HOME/bin/hdfs --daemon start namenode </dev/null >/tmp/namenode_boot.log 2>&1
setsid $HADOOP_HOME/bin/hdfs --daemon start datanode </dev/null >/tmp/datanode_boot.log 2>&1

echo "=== Khởi động Apache Zookeeper ==="
setsid /home/mitke/kafka/bin/zookeeper-server-start.sh /home/mitke/kafka/config/zookeeper.properties </dev/null >/tmp/zookeeper_boot.log 2>&1 &
sleep 4

echo "=== Khởi động Apache Kafka Broker ==="
setsid /home/mitke/kafka/bin/kafka-server-start.sh /home/mitke/kafka/config/server.properties </dev/null >/tmp/kafka_boot.log 2>&1 &
sleep 6

disown -a 2>/dev/null
echo "=== Trạng thái các tiến trình sau khi tách phiên ==="
jps -l
