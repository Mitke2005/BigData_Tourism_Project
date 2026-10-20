"""
spark/spark_batch_train.py
==========================
JOB BATCH HUẤN LUYỆN SPARK MLLIB & XUẤT STATE STORE KAFKA (KAFKA-FIRST LAMBDA ARCHITECTURE)
Quy trình thực thi chuẩn Phase 1:
1. Khởi tạo SparkSession với gói spark-sql-kafka-0-10.
2. Chụp snapshot offsets của topic 'tourism-reviews' tại thời điểm bắt đầu.
3. Đọc dữ liệu review từ Kafka bằng spark.read.format("kafka") (earliest -> end_offsets),
   dropDuplicates(["event_id"]), trích xuất synced_event_ids và tính review_sum, review_count theo STT.
4. Đọc Master Dataset bất biến từ HDFS processed_zone (kiểm tra STT duy nhất, tọa độ VN, rating [1..5]; không hard-code số dòng).
5. Huấn luyện K-Means (k=5) qua PySpark MLlib và LƯU MODEL lên HDFS analytics_zone:
   hdfs://.../tourism_data/analytics_zone/models/kmeans/job_id=<id>
6. Khai phá FP-Growth association rules, trích xuất TF-IDF Cosine Similarity và sinh lịch trình đa ngày (1..5 ngày).
7. Ghi Parquet kết quả vào HDFS analytics_zone (coalesce 1, overwrite theo job_id).
8. Xuất toàn bộ dữ liệu qua messaging.state_store.publish:
   - tourism-state-spots: từng điểm kèm job_id, base_rating, review_sum, review_count, cluster_id, trained_at.
   - tourism-state-batch: model:kmeans, model:rules, model:query_recs, itinerary:<region>:<days>.
   - tourism-state-batch CUỐI CÙNG: batch_commit (status COMMITTED, spots_count, itinerary_count, synced_event_ids, review_snapshot_offsets).
   - tourism-control: batch_job:<id> theo từng bước (RUNNING -> SUCCEEDED / FAILED).
9. Idempotent, thoát exit code != 0 khi gặp sự cố, in bảng tóm tắt chi tiết.
"""

import os
import sys
import time
import json
import argparse
from datetime import datetime
from typing import Dict, Any, List, Set, Optional

if sys.platform.startswith('win'):
    try:
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
        sys.stderr.reconfigure(encoding='utf-8', errors='replace')
    except Exception:
        pass

# Thêm đường dẫn project
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

# Biến môi trường đa nền tảng cho Hadoop & winutils trên Windows
HADOOP_HOME = os.environ.get("HADOOP_HOME", "C:/hadoop" if sys.platform.startswith('win') else "/usr/local/hadoop")
HADOOP_HOME = os.path.abspath(HADOOP_HOME).replace('\\', '/')
os.environ['HADOOP_HOME'] = HADOOP_HOME
os.environ['hadoop.home.dir'] = HADOOP_HOME
JAVA_HOME = "C:/jdk-17" if sys.platform.startswith('win') else os.environ.get("JAVA_HOME", "/usr/lib/jvm/java-17-openjdk-amd64")
os.environ['JAVA_HOME'] = JAVA_HOME
os.environ['HADOOP_USER_NAME'] = 'mitke'
if sys.platform.startswith('win'):
    bin_path = os.path.join(HADOOP_HOME, "bin").replace('/', '\\')
    java_bin = os.path.join(JAVA_HOME, "bin").replace('/', '\\')
    current_path = os.environ.get("PATH", "")
    if bin_path not in current_path:
        current_path = bin_path + os.pathsep + current_path
    if java_bin not in current_path:
        current_path = java_bin + os.pathsep + current_path
    os.environ["PATH"] = current_path

# Apache Spark & PySpark MLlib
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.sql.functions import col, from_json, count as spark_count, sum as spark_sum

# Kafka & State Store
from kafka import KafkaConsumer, TopicPartition
from messaging.state_store import (
    TOPIC_STATE_SPOTS,
    TOPIC_STATE_BATCH,
    TOPIC_REVIEWS,
    TOPIC_CONTROL,
    TOPIC_OUTPUT,
    ensure_topics,
    publish
)
from spark.tourism_engine import SparkTourismCoreEngine

DEFAULT_TARGET_REGIONS = [
    'Khánh Hòa', 'Lâm Đồng', 'Đà Nẵng', 'Hà Nội', 'Kiên Giang',
    'Quảng Ninh', 'Hồ Chí Minh', 'Quảng Nam', 'Ninh Bình', 'Cần Thơ', 'Bình Định'
]
TARGET_DAYS = [1, 2, 3, 4, 5]


def update_control_status(job_id: str, status: str, step: str = "", extra: Optional[Dict[str, Any]] = None, bootstrap_servers: Optional[str] = None):
    """Cập nhật trạng thái tiến trình job batch vào topic tourism-control."""
    payload = {
        "job_id": job_id,
        "status": status,
        "step": step,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    if extra:
        payload.update(extra)
    try:
        publish(
            topic=TOPIC_CONTROL,
            key=f"batch_job:{job_id}",
            value=payload,
            bootstrap_servers=bootstrap_servers
        )
    except Exception as e:
        print(f"[!] Cảnh báo cập nhật control status ({step}): {e}")


def get_kafka_review_end_offsets(bootstrap_servers: str) -> Dict[TopicPartition, int]:
    """Lấy end_offsets hiện tại của topic tourism-reviews tại thời điểm bắt đầu batch job."""
    consumer = KafkaConsumer(
        bootstrap_servers=bootstrap_servers,
        api_version=(2, 8, 1),
        request_timeout_ms=10000
    )
    try:
        partitions = consumer.partitions_for_topic(TOPIC_REVIEWS)
        if not partitions:
            return {}
        tps = [TopicPartition(TOPIC_REVIEWS, p) for p in sorted(partitions)]
        end_offsets = consumer.end_offsets(tps)
        return end_offsets
    finally:
        consumer.close()


def run_batch_training(
    job_id: Optional[str] = None,
    regions: Optional[List[str]] = None,
    hdfs_base: Optional[str] = None,
    kafka_bootstrap: Optional[str] = None
) -> Dict[str, Any]:
    """
    Thực thi toàn bộ chu trình Batch Training theo chuẩn Phase 1.
    """
    start_time = time.time()
    sync_job_id = job_id or f"job_{int(time.time() * 1000)}"
    hdfs_uri_base = hdfs_base or os.getenv("HDFS_URI", "hdfs://localhost:9000")
    bootstrap_servers = kafka_bootstrap or os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")

    print("=" * 80)
    print(f"  KHỞI CHẠY JOB BATCH SPARK MLLIB - KAFKA STATE STORE (Job ID: {sync_job_id})")
    print(f"  HDFS: {hdfs_uri_base} | Kafka: {bootstrap_servers}")
    print("=" * 80)

    # 1. Đảm bảo các Kafka topics đã được tạo đúng cấu hình chuẩn
    print("[*] [Bước 0/7] Kiểm tra và khởi tạo Kafka Topics theo đặc tả...")
    ensure_topics(bootstrap_servers=bootstrap_servers)

    update_control_status(
        sync_job_id,
        status="RUNNING",
        step="0/7: Initialized topics and capturing review snapshot offsets",
        bootstrap_servers=bootstrap_servers
    )

    # 2. Chụp snapshot offsets của topic tourism-reviews lúc bắt đầu
    print("[*] [Bước 1/7] Chụp snapshot end_offsets của topic 'tourism-reviews'...")
    end_offsets_map = get_kafka_review_end_offsets(bootstrap_servers)
    snapshot_offsets_json = {str(tp.partition): off for tp, off in end_offsets_map.items()}
    total_review_msgs = sum(end_offsets_map.values()) if end_offsets_map else 0
    print(f"[+] Snapshot offsets 'tourism-reviews': {snapshot_offsets_json} (Tổng offset tích lũy: {total_review_msgs})")

    # 3. Khởi tạo Apache SparkSession
    print("[*] [Bước 2/7] Khởi tạo Apache SparkSession (hỗ trợ Kafka & HDFS Native)...")
    spark = SparkSession.builder \
        .appName(f"VietnamTourism_BatchTrain_{sync_job_id}") \
        .master("local[*]") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.driver.memory", "2g") \
        .config("spark.hadoop.hadoop.home.dir", HADOOP_HOME) \
        .config("spark.driver.extraJavaOptions", f"-Dhadoop.home.dir={HADOOP_HOME}") \
        .config("spark.executor.extraJavaOptions", f"-Dhadoop.home.dir={HADOOP_HOME}") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        # 4. Đọc snapshot review từ Kafka bằng spark.read.format("kafka")
        print("[*] [Bước 3/7] Đọc snapshot reviews từ Kafka 'tourism-reviews' qua PySpark...")
        synced_event_ids: List[str] = []
        review_stats_by_stt: Dict[int, Dict[str, float]] = {}

        if total_review_msgs > 0:
            spark_end_offsets = json.dumps({TOPIC_REVIEWS: {str(tp.partition): off for tp, off in end_offsets_map.items()}})
            df_kafka_raw = spark.read.format("kafka") \
                .option("kafka.bootstrap.servers", bootstrap_servers) \
                .option("subscribe", TOPIC_REVIEWS) \
                .option("startingOffsets", "earliest") \
                .option("endingOffsets", spark_end_offsets) \
                .load()

            review_schema = StructType([
                StructField("event_id", StringType(), True),
                StructField("stt", IntegerType(), True),
                StructField("rating", DoubleType(), True),
                StructField("user_name", StringType(), True),
                StructField("review_text", StringType(), True),
                StructField("timestamp", StringType(), True)
            ])

            df_reviews_parsed = df_kafka_raw.select(
                from_json(col("value").cast("string"), review_schema).alias("data")
            ).select("data.*")

            # Khử trùng lặp theo event_id trong snapshot
            df_reviews_dedup = df_reviews_parsed.filter(col("event_id").isNotNull()).dropDuplicates(["event_id"]).cache()

            synced_event_ids = [row.event_id for row in df_reviews_dedup.select("event_id").collect() if row.event_id]

            # Tính review_sum và review_count theo từng STT
            agg_rows = df_reviews_dedup.groupBy("stt").agg(
                spark_sum("rating").alias("review_sum"),
                spark_count("rating").alias("review_count")
            ).collect()

            for r in agg_rows:
                if r["stt"] is not None:
                    review_stats_by_stt[int(r["stt"])] = {
                        "review_sum": float(r["review_sum"] or 0.0),
                        "review_count": int(r["review_count"] or 0)
                    }

            print(f"[+] Đã đọc snapshot {len(synced_event_ids)} reviews duy nhất từ Kafka, tổng hợp cho {len(review_stats_by_stt)} điểm đến.")
        else:
            print("[+] Topic 'tourism-reviews' hiện chưa có dữ liệu review (offsets = 0). Snapshot rỗng.")

        update_control_status(
            sync_job_id,
            status="RUNNING",
            step="3/7: Ingesting master dataset from HDFS and verifying quality",
            extra={"reviews_in_snapshot": len(synced_event_ids)},
            bootstrap_servers=bootstrap_servers
        )

        # 5. Đọc Master Dataset từ HDFS processed_zone & kiểm định chất lượng
        master_hdfs_path = f"{hdfs_uri_base}/tourism_data/processed_zone/tourism_dataset_clean.csv"
        print(f"[*] [Bước 4/7] Đọc Master Dataset từ HDFS ({master_hdfs_path})...")
        master_df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .option("multiLine", "true") \
            .option("quote", "\"") \
            .option("escape", "\"") \
            .csv(master_hdfs_path)

        total_master_count = master_df.count()
        if total_master_count == 0:
            raise ValueError(f"Master Dataset trên HDFS trống rỗng ({master_hdfs_path})!")

        unique_stt_count = master_df.select("STT").distinct().count()
        if unique_stt_count != total_master_count:
            raise ValueError(f"STT trong Master Dataset không duy nhất! Tổng dòng: {total_master_count}, STT duy nhất: {unique_stt_count}")

        # Kiểm tra tọa độ trong phạm vi lãnh thổ Việt Nam
        invalid_coords = master_df.filter(
            (col("Latitude") < 8.0) | (col("Latitude") > 24.0) |
            (col("Longitude") < 102.0) | (col("Longitude") > 118.0)
        ).count()
        if invalid_coords > 0:
            raise ValueError(f"Phát hiện {invalid_coords} bản ghi có tọa độ nằm ngoài phạm vi Việt Nam [8..24°N, 102..118°E]!")

        # Kiểm tra rating trong khoảng [1.0, 5.0]
        invalid_ratings = master_df.filter(
            (col("Rating") < 1.0) | (col("Rating") > 5.0)
        ).count()
        if invalid_ratings > 0:
            raise ValueError(f"Phát hiện {invalid_ratings} bản ghi có Rating ngoài khoảng hợp lệ [1.0, 5.0]!")

        print(f"[+] [Data Quality PASS] Master Dataset đạt chuẩn 100%: {total_master_count} điểm, STT duy nhất, Tọa độ & Rating hợp lệ.")

        update_control_status(
            sync_job_id,
            status="RUNNING",
            step="4/7: Training MLlib models (K-Means, FP-Growth, TF-IDF)",
            bootstrap_servers=bootstrap_servers
        )

        # 6. Huấn luyện mô hình phân tán qua SparkTourismCoreEngine
        print("[*] [Bước 5/7] Huấn luyện các mô hình PySpark MLlib phân tán...")
        engine = SparkTourismCoreEngine(spark=spark, df=master_df)

        # Lưu K-Means Model vào HDFS analytics_zone
        hdfs_kmeans_dir = f"{hdfs_uri_base}/tourism_data/analytics_zone/models/kmeans/job_id={sync_job_id}"
        print(f"[*] Đang lưu mô hình K-Means vào HDFS: {hdfs_kmeans_dir}...")
        engine.kmeans_model.write().overwrite().save(hdfs_kmeans_dir)
        print(f"[+] [HDFS ML Model] Đã lưu K-Means Model thành công tại: {hdfs_kmeans_dir}")

        trained_at_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Cập nhật số lượt review và tổng điểm vào catalog mà TUYỆT ĐỐI KHÔNG GHI ĐÈ MASTER DATASET
        for spot in engine.catalog:
            s_stt = int(spot["stt"])
            st_data = review_stats_by_stt.get(s_stt, {"review_sum": 0.0, "review_count": 0})
            spot["base_rating"] = float(spot.get("base_rating", spot.get("rating", 4.5)))
            spot["review_sum"] = round(float(st_data["review_sum"]), 2)
            spot["review_count"] = int(st_data["review_count"])
            spot["job_id"] = sync_job_id
            spot["trained_at"] = trained_at_str

        # 7. Sinh lịch trình đa ngày 1..5 ngày
        selected_regions = regions if regions else DEFAULT_TARGET_REGIONS
        print(f"[*] Sinh lịch trình đa ngày (1..5 ngày) cho các tỉnh: {selected_regions}...")
        itineraries: Dict[str, Dict[str, Any]] = {}
        for reg in selected_regions:
            for days in TARGET_DAYS:
                itin_res = engine.generate_itinerary(region=reg, days=days)
                if itin_res:
                    itin_key = f"itinerary:{reg.strip().lower()}:{days}"
                    itineraries[itin_key] = {
                        "job_id": sync_job_id,
                        "region": reg,
                        "days": days,
                        "itinerary": itin_res,
                        "trained_at": trained_at_str
                    }

        print(f"[+] Đã sinh thành công {len(itineraries)} lịch trình tối ưu.")

        # 8. Ghi Parquet kết quả phân tán vào HDFS analytics_zone (coalesce 1, overwrite theo job_id)
        print("[*] Ghi Parquet kết quả phân tán vào HDFS analytics_zone...")
        analytics_spots_path = f"{hdfs_uri_base}/tourism_data/analytics_zone/spots/job_id={sync_job_id}"
        engine.clustered_df.drop('features').coalesce(1).write.mode("overwrite").parquet(analytics_spots_path)
        print(f"[+] [Parquet Spots] Đã ghi Parquet kết quả phân cụm: {analytics_spots_path}")

        analytics_rules_path = f"{hdfs_uri_base}/tourism_data/analytics_zone/association_rules/job_id={sync_job_id}"
        engine.fp_model.associationRules.coalesce(1).write.mode("overwrite").parquet(analytics_rules_path)
        print(f"[+] [Parquet Rules] Đã ghi Parquet luật kết hợp: {analytics_rules_path}")

        update_control_status(
            sync_job_id,
            status="RUNNING",
            step="5/7: Publishing state topics to Kafka (spots, models, itineraries)",
            bootstrap_servers=bootstrap_servers
        )

        # 9. Xuất State sang Kafka compacted topics bằng state_store.publish
        print("[*] [Bước 6/7] Xuất bản dữ liệu sang các compacted State Topics...")

        # A. tourism-state-spots
        print(f"[*] Xuất {len(engine.catalog)} điểm du lịch sang '{TOPIC_STATE_SPOTS}'...")
        for spot in engine.catalog:
            publish(
                topic=TOPIC_STATE_SPOTS,
                key=str(spot["stt"]),
                value=spot,
                bootstrap_servers=bootstrap_servers
            )

        # B. tourism-state-batch: model:kmeans
        kmeans_payload = {
            "job_id": sync_job_id,
            "version": sync_job_id,
            "k": len(engine.centers),
            "centers": engine.centers,
            "cluster_meta": engine.cluster_meta,
            "hdfs_model_path": hdfs_kmeans_dir,
            "trained_at": trained_at_str
        }
        publish(
            topic=TOPIC_STATE_BATCH,
            key="model:kmeans",
            value=kmeans_payload,
            bootstrap_servers=bootstrap_servers
        )

        # C. tourism-state-batch: model:rules
        rules_payload = {
            "job_id": sync_job_id,
            "rules": engine.association_rules,
            "total_rules": len(engine.association_rules),
            "trained_at": trained_at_str
        }
        publish(
            topic=TOPIC_STATE_BATCH,
            key="model:rules",
            value=rules_payload,
            bootstrap_servers=bootstrap_servers
        )

        # D. tourism-state-batch: model:query_recs
        query_payload = {
            "job_id": sync_job_id,
            "query_recommendations": engine.query_recommendations,
            "trained_at": trained_at_str
        }
        publish(
            topic=TOPIC_STATE_BATCH,
            key="model:query_recs",
            value=query_payload,
            bootstrap_servers=bootstrap_servers
        )

        # E. tourism-state-batch: itineraries
        for itin_key, itin_val in itineraries.items():
            publish(
                topic=TOPIC_STATE_BATCH,
                key=itin_key,
                value=itin_val,
                bootstrap_servers=bootstrap_servers
            )

        # F. CUỐI CÙNG: batch_commit
        print(f"[*] Xuất bản bản ghi 'batch_commit' nguyên tử sang '{TOPIC_STATE_BATCH}'...")
        commit_payload = {
            "job_id": sync_job_id,
            "status": "COMMITTED",
            "spots_count": len(engine.catalog),
            "itinerary_count": len(itineraries),
            "synced_event_ids": synced_event_ids,
            "review_snapshot_offsets": snapshot_offsets_json,
            "trained_at": trained_at_str
        }
        publish(
            topic=TOPIC_STATE_BATCH,
            key="batch_commit",
            value=commit_payload,
            bootstrap_servers=bootstrap_servers
        )

        # G. Báo sự kiện refresh sang tourism-output để Web Dashboard cập nhật kịp thời
        output_event = {
            "action": "SPARK_BATCH_CATALOG_REFRESH",
            "job_id": sync_job_id,
            "spots_count": len(engine.catalog),
            "itinerary_count": len(itineraries),
            "timestamp": trained_at_str
        }
        try:
            publish(
                topic=TOPIC_OUTPUT,
                key="BATCH_REFRESH",
                value=output_event,
                bootstrap_servers=bootstrap_servers
            )
        except Exception as e_out:
            print(f"[!] Cảnh báo xuất tourism-output: {e_out}")

        duration = round(time.time() - start_time, 2)

        # Cập nhật tourism-control trạng thái SUCCEEDED
        update_control_status(
            sync_job_id,
            status="SUCCEEDED",
            step="7/7: Batch training and Kafka state store publish successfully completed",
            extra={
                "spots_count": len(engine.catalog),
                "clusters_count": len(engine.centers),
                "rules_count": len(engine.association_rules),
                "itinerary_count": len(itineraries),
                "reviews_snapshot_count": len(synced_event_ids),
                "duration_seconds": duration
            },
            bootstrap_servers=bootstrap_servers
        )

        print("=" * 80)
        print("  TÓM TẮT KẾT QUẢ HUẤN LUYỆN BATCH LAYER (PHASE 1):")
        print(f"  - Mã phiên (Job ID): {sync_job_id}")
        print(f"  - Số điểm du lịch (spots): {len(engine.catalog)}")
        print(f"  - Số cụm K-Means (k=5): {len(engine.centers)}")
        print(f"  - Số luật kết hợp FP-Growth: {len(engine.association_rules)}")
        print(f"  - Số lịch trình đa ngày sinh ra: {len(itineraries)}")
        print(f"  - Số review trong snapshot Kafka: {len(synced_event_ids)}")
        print(f"  - Thời gian thực thi: {duration} giây")
        print(f"  - Trạng thái: SUCCEEDED (ĐÃ GỬI BATCH_COMMIT NGUYÊN TỬ)")
        print("=" * 80)

        return {
            "success": True,
            "job_id": sync_job_id,
            "spots_count": len(engine.catalog),
            "clusters_count": len(engine.centers),
            "rules_count": len(engine.association_rules),
            "itinerary_count": len(itineraries),
            "reviews_snapshot_count": len(synced_event_ids),
            "duration": duration,
            "trained_at": trained_at_str
        }

    except Exception as e:
        update_control_status(
            sync_job_id,
            status="FAILED",
            step="FAILED",
            extra={"error": str(e)},
            bootstrap_servers=bootstrap_servers
        )
        print(f"\n[-] LỖI NGHIÊM TRỌNG TRONG JOB BATCH TRAINING: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        if spark is not None:
            try:
                spark.stop()
            except Exception:
                pass


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Chạy Job Batch Spark MLlib & Xuất Kafka State Store")
    parser.add_argument("--job-id", type=str, default=None, help="Mã phiên huấn luyện (Job ID)")
    parser.add_argument("--regions", type=str, default=None, help="Danh sách tỉnh thành (ngăn cách bởi dấu phẩy, mặc định: 11 tỉnh trọng điểm)")
    parser.add_argument("--hdfs-base", type=str, default=None, help="Đường dẫn HDFS gốc (mặc định: hdfs://localhost:9000)")
    parser.add_argument("--kafka-bootstrap", type=str, default=None, help="Kafka Bootstrap Servers (mặc định: localhost:9092)")

    args = parser.parse_args()

    parsed_regions = None
    if args.regions:
        parsed_regions = [r.strip() for r in args.regions.split(",") if r.strip()]

    try:
        res = run_batch_training(
            job_id=args.job_id,
            regions=parsed_regions,
            hdfs_base=args.hdfs_base,
            kafka_bootstrap=args.kafka_bootstrap
        )
        sys.exit(0 if res.get("success") else 1)
    except Exception as e:
        import traceback
        traceback.print_exc()
        sys.exit(1)
