#!/bin/bash

export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export HADOOP_HOME=/home/nhu/hadoop
export PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH

echo "=== Khởi động Hadoop HDFS Daemons ==="

# 1. Khởi động NameNode nếu chưa chạy
if ! jps | grep -q NameNode; then
    echo "Khởi động NameNode..."
    $HADOOP_HOME/bin/hdfs --daemon start namenode
else
    echo "NameNode đã đang chạy."
fi

# 2. Khởi động DataNode nếu chưa chạy
if ! jps | grep -q DataNode; then
    echo "Khởi động DataNode..."
    $HADOOP_HOME/bin/hdfs --daemon start datanode
else
    echo "DataNode đã đang chạy."
fi

sleep 3
echo "=== Các tiến trình Hadoop đang chạy (JPS) ==="
jps

echo "Kiểm tra cổng HDFS Web UI: http://localhost:9870"
echo "Kiểm tra cổng HDFS IPC: hdfs://localhost:9000"
