#!/bin/bash

export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH
export KAFKA_HOME=/home/nhu/kafka

# Check if Kafka is already running
if pgrep -f "kafka.Kafka" > /dev/null; then
    echo "Kafka is already running!"
    exit 0
fi

echo "=== Khởi động Kafka Broker (Port 9092) ==="
mkdir -p $KAFKA_HOME/logs
nohup $KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server.properties > $KAFKA_HOME/logs/kafka_console.log 2>&1 &

echo "Đang đợi Kafka khởi động..."
for i in {1..15}; do
    if grep -q "Kafka Server started" $KAFKA_HOME/logs/kafka_console.log 2>/dev/null || \
       grep -q "Transitioning from STARTING to STARTED" $KAFKA_HOME/logs/kafka_console.log 2>/dev/null; then
        echo "=== Kafka Broker đã khởi động thành công trên cổng 9092! ==="
        exit 0
    fi
    sleep 1
done

echo "Kiểm tra tiến trình Kafka:"
ps aux | grep -i "kafka.Kafka" | grep -v grep || true
