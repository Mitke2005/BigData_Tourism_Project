#!/bin/bash
set -e

# Thiết lập đường dẫn môi trường Hadoop & Java
export JAVA_HOME=${JAVA_HOME:-/usr/lib/jvm/java-8-openjdk-amd64}
export HADOOP_HOME=${HADOOP_HOME:-/home/mitke/hadoop}
export PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

echo "=== Khởi tạo cấu trúc phân tầng Data Lake trên HDFS ==="
$HADOOP_HOME/bin/hdfs dfs -mkdir -p /tourism_data/raw_zone
$HADOOP_HOME/bin/hdfs dfs -mkdir -p /tourism_data/processed_zone
$HADOOP_HOME/bin/hdfs dfs -mkdir -p /tourism_data/analytics_zone

echo "=== Đẩy tệp dữ liệu du lịch lên HDFS ==="
if [ -f "$PROJECT_DIR/data/DataSet_Enriched_Keywords.xlsx" ]; then
    $HADOOP_HOME/bin/hdfs dfs -put -f "$PROJECT_DIR/data/DataSet_Enriched_Keywords.xlsx" /tourism_data/raw_zone/
fi
if [ -f "$PROJECT_DIR/data/tourism_dataset_clean.csv" ]; then
    $HADOOP_HOME/bin/hdfs dfs -put -f "$PROJECT_DIR/data/tourism_dataset_clean.csv" /tourism_data/processed_zone/
fi

echo "=== Kiểm tra danh mục lưu trữ trên HDFS ==="
$HADOOP_HOME/bin/hdfs dfs -ls -R /tourism_data
echo "=== Nạp dữ liệu HDFS hoàn tất! ==="
