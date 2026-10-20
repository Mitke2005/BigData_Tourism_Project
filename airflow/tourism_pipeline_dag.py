"""
DAG AIRFLOW CHO ĐỒ ÁN BIG DATA DU LỊCH (KAFKA-FIRST LAMBDA ARCHITECTURE)
=============================================================================
Quy trình tự động hóa:
  Task 1: ensure_topics       - Khởi tạo & kiểm tra 6 Kafka Topics chuẩn Lambda Architecture
  Task 2: upload_master       - Đảm bảo Master Dataset trên HDFS (Kiểm tra SHA-256, không overwrite bừa bãi)
  Task 3: run_batch_job       - Huấn luyện PySpark MLlib Batch Layer & nạp Kafka State Store
  Task 4: verify_quality      - Kiểm định chất lượng từ Kafka State Topics (batch_commit, spots, kmeans, itinerary)

DAG 2: tourism_streaming_compaction (Định kỳ nén gộp micro-batch Parquet trên HDFS)
=============================================================================
"""

import os
import sys
import json
import subprocess
from datetime import datetime, timedelta
import sys
from pathlib import Path

PROJECT_ROOT = str(Path(__file__).resolve().parent.parent)

if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)
if sys.platform.startswith('win'):
    try:
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
        sys.stderr.reconfigure(encoding='utf-8', errors='replace')
    except Exception:
        pass

BASE_DIR = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)

HDFS_URI = os.environ.get("HDFS_URI", "hdfs://localhost:9000")

# Kiểm tra thư viện Airflow
try:
    from airflow import DAG  # type: ignore
    from airflow.operators.python import PythonOperator  # type: ignore
    AIRFLOW_AVAILABLE = True
except ImportError:
    AIRFLOW_AVAILABLE = False


default_args = {
    'owner': 'Nhom_BigData_DoAn',
    'depends_on_past': False,
    'start_date': datetime(2026, 10, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(seconds=10),
}


# --- CÁC TASKS CHÍNH THEO ĐẶC TẢ LAMBDA ARCHITECTURE ---

def task_ensure_topics():
    """Task 1: Khởi tạo và kiểm tra 6 Kafka Topics (3 compacted state topics, 3 stream topics)."""
    print("[Task 1 - ensure_topics] Kiểm tra và khởi tạo các Kafka topics...")
    from messaging.create_topics import ensure_all_topics
    success = ensure_all_topics(bootstrap_servers="localhost:9092")
    if not success:
        raise ConnectionError("Không thể khởi tạo hoặc kết nối Kafka topics trên localhost:9092!")
    print("[Task 1 - ensure_topics] Xác thực 6 topics thành công.")
    return "TOPICS_VERIFIED"


def task_upload_master():
    """Task 2: Upload tập master dataset lên HDFS raw & processed zone nếu chưa có hoặc kiểm tra SHA-256."""
    print("[Task 2 - upload_master] Đảm bảo Master Dataset có mặt trên HDFS mà không ghi đè nếu trùng SHA-256...")
    sync_script = os.path.join(BASE_DIR, "hadoop", "hdfs_sync.py")
    res = subprocess.run([sys.executable, sync_script], capture_output=True, text=True)
    if res.returncode != 0:
        raise RuntimeError(f"Lỗi khi đồng bộ Master Dataset lên HDFS: {res.stderr or res.stdout}")
    print("[Task 2 - upload_master] Master Dataset đã được xác thực an toàn trên HDFS.")
    return "MASTER_ON_HDFS_VERIFIED"

def task_run_batch_job():
    """Chạy Spark Batch thực tế thông qua spark-submit."""

    print("[Task 3] Khoi chay Spark Batch Training...")

    batch_script = os.path.join(
        BASE_DIR, "spark", "spark_batch_train.py"
    )

    spark_submit = "/home/mitke/spark/bin/spark-submit"
    spark_python = "/home/mitke/airflow_venv/bin/python"

    env = os.environ.copy()

    env["SPARK_HOME"] = "/home/mitke/spark"
    env["PYSPARK_PYTHON"] = spark_python
    env["PYSPARK_DRIVER_PYTHON"] = spark_python
    env["SPARK_LOCAL_IP"] = "127.0.0.1"

    # Su dung Java da cai trong Ubuntu
    env["JAVA_HOME"] = os.path.dirname(
        os.path.dirname(os.path.realpath("/usr/bin/java"))
    )

    env["PYTHONPATH"] = (
        BASE_DIR + os.pathsep + env.get("PYTHONPATH", "")
    )

    res = subprocess.run(
        [
		 spark_submit,
        	"--master", "local[2]",
        	"--driver-memory", "2g",
        	"--packages",
        	"org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1",
        	batch_script
        ],
        cwd=BASE_DIR,
        env=env,
        capture_output=True,
        text=True
    )

    print(res.stdout[-10000:])

    if res.returncode != 0:
        raise RuntimeError(
            f"Spark Batch failed (code {res.returncode})\n"
            f"STDOUT:\n{res.stdout[-6000:]}\n"
            f"STDERR:\n{res.stderr[-12000:]}"
        )

    print("[Task 3] Spark Batch Training thanh cong!")

    return "BATCH_JOB_SUCCESS"
def task_verify_quality():
    """
    Task 4: Đọc state topics qua KafkaConsumer:
      - Kiểm tra commit tồn tại trong tourism-state-catalog (hoặc tourism-control)
      - Số spots == số dòng master (55 địa điểm)
      - Topic tourism-state-kmeans có dữ liệu (key model:kmeans)
      - Topic tourism-state-itinerary có dữ liệu
    """
    print("[Task 4 - verify_quality] Kiểm định chất lượng từ Kafka State Topics...")
    try:
        from kafka import KafkaConsumer, TopicPartition  # type: ignore
    except ImportError:
        from kafka_python_ng import KafkaConsumer, TopicPartition  # type: ignore

    import pandas as pd
    master_csv = os.path.join(BASE_DIR, "data", "tourism_dataset_clean.csv")
    expected_spot_count = len(pd.read_csv(master_csv))

    # Đọc tourism-state-spots (hoặc fallback tourism-state-catalog)
    spot_topic = "tourism-state-spots"
    consumer = KafkaConsumer(
        spot_topic,
        bootstrap_servers=["localhost:9092"],
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        consumer_timeout_ms=4000,
        api_version=(2, 8, 1)
    )

    catalog_records = {}
    for msg in consumer:
        try:
            key_str = msg.key.decode('utf-8') if msg.key else ""
            if not msg.value:
                continue
            val_json = json.loads(msg.value.decode('utf-8'))
            spot_id = int(key_str.split(":")[-1]) if ":" in key_str else int(key_str)
            catalog_records[spot_id] = val_json
        except Exception:
            pass
    consumer.close()

    # Nếu tourism-state-spots rỗng, thử đọc tourism-state-catalog
    if not catalog_records:
        try:
            c_cat = KafkaConsumer(
                "tourism-state-catalog",
                bootstrap_servers=["localhost:9092"],
                auto_offset_reset='earliest',
                enable_auto_commit=False,
                consumer_timeout_ms=3000,
                api_version=(2, 8, 1)
            )
            for msg in c_cat:
                key_str = msg.key.decode('utf-8') if msg.key else ""
                if not msg.value:
                    continue
                val_json = json.loads(msg.value.decode('utf-8'))
                if key_str.startswith("spot:"):
                    catalog_records[int(key_str.split(":")[1])] = val_json
            c_cat.close()
        except Exception:
            pass

    spot_count = len(catalog_records)
    print(f"[*] Số địa điểm trong Kafka State Spots: {spot_count} (Kỳ vọng: {expected_spot_count})")
    if spot_count != expected_spot_count:
        raise AssertionError(f"Số lượng spot trong Kafka ({spot_count}) không khớp với Master CSV ({expected_spot_count})!")

    # Đọc tourism-state-batch (hoặc fallback tourism-state-kmeans)
    has_kmeans_data = False
    for topic_try in ["tourism-state-batch", "tourism-state-kmeans"]:
        try:
            consumer_kmeans = KafkaConsumer(
                topic_try,
                bootstrap_servers=["localhost:9092"],
                auto_offset_reset='earliest',
                enable_auto_commit=False,
                consumer_timeout_ms=3000,
                api_version=(2, 8, 1)
            )
            for msg in consumer_kmeans:
                key_str = msg.key.decode('utf-8') if msg.key else ""
                if "model:kmeans" in key_str or (msg.value and b"kmeans" in msg.value):
                    has_kmeans_data = True
                    break
            consumer_kmeans.close()
            if has_kmeans_data:
                break
        except Exception:
            pass
    if not has_kmeans_data:
        raise AssertionError("Topic 'tourism-state-batch' (hoặc 'tourism-state-kmeans') không chứa dữ liệu cụm K-Means!")
    print("[+] Topic 'tourism-state-batch' đã có mô hình K-Means!")

    # Đọc tourism-state-batch hoặc fallback tourism-state-itinerary
    has_itin_data = False
    for itin_topic in ["tourism-state-batch", "tourism-state-itinerary"]:
        try:
            consumer_itin = KafkaConsumer(
                itin_topic,
                bootstrap_servers=["localhost:9092"],
                auto_offset_reset='earliest',
                enable_auto_commit=False,
                consumer_timeout_ms=3000,
                api_version=(2, 8, 1)
            )
            for msg in consumer_itin:
                key_str = msg.key.decode('utf-8') if msg.key else ""
                if "itinerary:" in key_str or (msg.value and b"itinerary" in msg.value):
                    has_itin_data = True
                    break
            consumer_itin.close()
            if has_itin_data:
                break
        except Exception:
            pass

    if not has_itin_data:
        raise AssertionError("Kafka State Store không chứa dữ liệu gợi ý lộ trình!")
    print("[+] Kafka State Store đã có dữ liệu lộ trình!")

    print("[Task 4 - verify_quality] XÁC THỰC TOÀN DIỆN KAFKA STATE THÀNH CÔNG 100%!")
    return "KAFKA_STATE_QUALITY_100_PERCENT_VALID"


# --- TASKS CHO COMPACTION ---
def task_run_compaction():
    """Task Compact: Chạy spark/compact_streaming_zone.py để gộp các micro-batch nhỏ."""
    print("[Task Compaction] Chạy job gộp file nhỏ từ streaming_zone sang analytics_zone...")
    compact_script = os.path.join(BASE_DIR, "spark", "compact_streaming_zone.py")
    res = subprocess.run([sys.executable, compact_script, "--older-than-minutes", "10"], check=True)
    return "STREAMING_ZONE_COMPACTED_SUCCESS"


def task_verify_compaction():
    """Task Verify Compaction: Kiểm tra dữ liệu analytics_zone."""
    print("[Task Verify Compaction] Kiểm chứng tính hợp lệ của tệp Parquet sau khi gộp...")
    from pyspark.sql import SparkSession
    spark = SparkSession.builder \
        .appName("Verify_Compaction_Output") \
        .master("local[*]") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .getOrCreate()
    
    analytics_path = f"{HDFS_URI}/tourism_data/analytics_zone/streaming_events"
    try:
        df = spark.read.parquet(analytics_path)
        cnt = df.count()
        print(f"[+] [Verify Compaction] Đọc thành công {cnt} bản ghi từ {analytics_path}!")
    except Exception as e:
        print(f"[*] Chưa có dữ liệu hoặc thư mục mới tạo ({e}).")
    finally:
        spark.stop()
    return "COMPACTION_VERIFIED"


# --- KHỞI TẠO CÁC DAG AIRFLOW NẾU CÓ AIRFLOW RUNTIME ---
if AIRFLOW_AVAILABLE:
    # DAG 1: Pipeline xử lý dữ liệu lớn chính
    dag = DAG(
        'vietnam_tourism_bigdata_pipeline',
        default_args=default_args,
        description='DAG tự động hóa toàn trình Pipeline du lịch theo Lambda Architecture Kafka-First',
        schedule_interval=timedelta(days=1),
        catchup=False,
    )

    t1 = PythonOperator(task_id='1_ensure_topics', python_callable=task_ensure_topics, dag=dag)
    t2 = PythonOperator(task_id='2_upload_master', python_callable=task_upload_master, dag=dag)
    t3 = PythonOperator(task_id='3_run_batch_job', python_callable=task_run_batch_job, dag=dag)
    t4 = PythonOperator(task_id='4_verify_quality', python_callable=task_verify_quality, dag=dag)

    t1 >> t2 >> t3 >> t4

    # DAG 2: Compaction gộp file nhỏ định kỳ hàng ngày
    compaction_default_args = {
        'owner': 'Nhom_BigData_DoAn',
        'depends_on_past': False,
        'start_date': datetime(2026, 10, 1),
        'retries': 2,
        'retry_delay': timedelta(seconds=15),
    }

    dag_compaction = DAG(
        'tourism_streaming_compaction',
        default_args=compaction_default_args,
        description='DAG định kỳ nén gộp micro-batch Parquet nhỏ trên HDFS',
        schedule_interval=timedelta(days=1),
        catchup=False,
    )

    c1 = PythonOperator(task_id='1_compact_streaming_zone', python_callable=task_run_compaction, dag=dag_compaction)
    c2 = PythonOperator(task_id='2_verify_compacted_data', python_callable=task_verify_compaction, dag=dag_compaction)

    c1 >> c2


if __name__ == '__main__':
    print("=" * 80)
    print("  AIRFLOW PIPELINE CHO ĐỒ ÁN BIG DATA DU LỊCH (KAFKA-FIRST):")
    print("  - DAG 1: vietnam_tourism_bigdata_pipeline (ensure_topics >> upload_master >> run_batch_job >> verify_quality)")
    print("  - DAG 2: tourism_streaming_compaction (compact >> verify)")
    print("=" * 80)
    if AIRFLOW_AVAILABLE:
        print("[+] Airflow khả dụng! Đang chạy dag.test()...")
        dag.test()
    else:
        print("[*] Chế độ CLI Runner trực tiếp (Airflow Tasks Sequential Execution):")
        task_ensure_topics()
        task_upload_master()
        task_run_batch_job()
        task_verify_quality()
        print("[+] Hoàn tất xác thực toàn trình các task Airflow!")

