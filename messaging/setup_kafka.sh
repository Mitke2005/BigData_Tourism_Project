#!/bin/bash
set -e

export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH
export KAFKA_HOME=/home/nhu/kafka

echo "=== Cấu hình Kafka KRaft Cluster ==="
rm -rf /tmp/kraft-combined-logs

CLUSTER_ID=$($KAFKA_HOME/bin/kafka-storage.sh random-uuid)
echo "Generated Cluster ID: $CLUSTER_ID"

$KAFKA_HOME/bin/kafka-storage.sh format --standalone -t $CLUSTER_ID -c $KAFKA_HOME/config/server.properties

echo "=== Định dạng Kafka KRaft thành công! ==="
