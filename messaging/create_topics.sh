#!/bin/bash
# =============================================================================
# KHỞI TẠO VÀ CẤU HÌNH KAFKA TOPICS CHO HỆ THỐNG LAMBDA ARCHITECTURE (KAFKA-FIRST)
# =============================================================================
export KAFKA_HOME=${KAFKA_HOME:-/home/nhu/kafka}
BOOTSTRAP=${KAFKA_BOOTSTRAP:-localhost:9092}

echo "=== Khởi tạo Kafka Topics cho Hệ thống Lambda Architecture (Kafka-First) ==="

# 1. Topic nhận sự kiện thô từ Web/Producer (3 Partitions, delete)
$KAFKA_HOME/bin/kafka-topics.sh --create --if-not-exists \
    --bootstrap-server $BOOTSTRAP \
    --replication-factor 1 \
    --partitions 3 \
    --config cleanup.policy=delete \
    --topic tourism-input

# 2. Topic phát sự kiện đã làm giàu cho Web SSE (3 Partitions, delete)
$KAFKA_HOME/bin/kafka-topics.sh --create --if-not-exists \
    --bootstrap-server $BOOTSTRAP \
    --replication-factor 1 \
    --partitions 3 \
    --config cleanup.policy=delete \
    --topic tourism-output

# 3. Topic log đánh giá thời gian thực chuẩn (3 Partitions, compact, retention.ms=-1)
$KAFKA_HOME/bin/kafka-topics.sh --create --if-not-exists \
    --bootstrap-server $BOOTSTRAP \
    --replication-factor 1 \
    --partitions 3 \
    --config cleanup.policy=compact \
    --config retention.ms=-1 \
    --topic tourism-reviews

# 4. Topic lưu Batch View từng địa điểm (1 Partition, compact, max.message.bytes=10MB)
$KAFKA_HOME/bin/kafka-topics.sh --create --if-not-exists \
    --bootstrap-server $BOOTSTRAP \
    --replication-factor 1 \
    --partitions 1 \
    --config cleanup.policy=compact \
    --config max.message.bytes=10485760 \
    --topic tourism-state-spots

# 5. Topic lưu Mô hình, Lịch trình và batch_commit (1 Partition, compact, max.message.bytes=10MB)
$KAFKA_HOME/bin/kafka-topics.sh --create --if-not-exists \
    --bootstrap-server $BOOTSTRAP \
    --replication-factor 1 \
    --partitions 1 \
    --config cleanup.policy=compact \
    --config max.message.bytes=10485760 \
    --topic tourism-state-batch

# 6. Topic điều khiển nhịp tim và trạng thái công việc (1 Partition, compact)
$KAFKA_HOME/bin/kafka-topics.sh --create --if-not-exists \
    --bootstrap-server $BOOTSTRAP \
    --replication-factor 1 \
    --partitions 1 \
    --config cleanup.policy=compact \
    --topic tourism-control

# 7. Topic chứa sự kiện lỗi (1 Partition, delete)
$KAFKA_HOME/bin/kafka-topics.sh --create --if-not-exists \
    --bootstrap-server $BOOTSTRAP \
    --replication-factor 1 \
    --partitions 1 \
    --config cleanup.policy=delete \
    --topic tourism-dead-letter

echo "=== Kiểm tra cấu hình chi tiết các Kafka Topics ==="
for t in tourism-input tourism-output tourism-reviews tourism-state-spots tourism-state-batch tourism-control tourism-dead-letter; do
    echo "--- Topic: $t ---"
    $KAFKA_HOME/bin/kafka-topics.sh --describe --topic $t --bootstrap-server $BOOTSTRAP
done
