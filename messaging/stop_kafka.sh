#!/bin/bash
export KAFKA_HOME=/home/nhu/kafka

echo "=== Dừng Kafka Broker ==="
$KAFKA_HOME/bin/kafka-server-stop.sh || true
pkill -f "kafka.Kafka" || true
echo "Kafka đã được dừng."
