#!/bin/bash

export HADOOP_HOME=/home/nhu/hadoop
export PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH

echo "=== Dừng Hadoop HDFS Daemons ==="
$HADOOP_HOME/bin/hdfs --daemon stop datanode || true
$HADOOP_HOME/bin/hdfs --daemon stop namenode || true

sleep 2
echo "=== Các tiến trình còn lại (JPS) ==="
jps
