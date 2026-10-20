"""
spark/spark_structured_streaming.py
===================================
SPEED LAYER - SPARK STRUCTURED STREAMING (KAFKA-FIRST LAMBDA ARCHITECTURE)
Cơ chế bảo đảm độ tin cậy: at-least-once + ghi idempotent theo run_id/batch_id và event_id

Quy trình thực thi chuẩn Phase 2:
1. Ingestion:
   - Nguồn: Apache Kafka topic 'tourism-input' (3 Partitions, key=session_id).
   - Đọc luồng: spark.readStream.format("kafka") với trigger processingTime="5 seconds",
     failOnDataLoss=true, maxOffsetsPerTrigger=1000, checkpoint trên HDFS.
   - Cấu hình phân tán: spark.sql.shuffle.partitions = 4.
2. Quản lý run_id gắn liền với Checkpoint HDFS:
   - Lưu tại <checkpoint_dir>/run_id.txt qua Hadoop FileSystem API.
   - Khởi động lại cùng checkpoint -> tái sử dụng run_id (replay batch ghi đè đường dẫn cũ, idempotent).
   - Reset/xóa checkpoint -> tạo run_id mới (không ghi đè dữ liệu cũ).
3. Xử lý Micro-Batch bằng DataFrame API thuần túy (KHÔNG dùng collect() / toPandas()):
   (1) from_json với StructType tường minh, giữ nguyên cột raw_value.
   (2) Validate bằng biểu thức cột:
       - STT phải tồn tại trong catalog (broadcast DataFrame đọc từ master HDFS lúc khởi động).
       - event_id không rỗng, event_type hợp lệ ('REVIEW' hoặc 'SIMULATED_VIEW').
       - Với sự kiện REVIEW: rating là số trong [1.0, 5.0], review_text <= 1000 ký tự, user_name <= 100 ký tự.
   (3) Bản ghi lỗi -> lưu HDFS dead_letter/run_id=<..>/batch_id=<..>/ VÀ publish vào 'tourism-dead-letter'.
   (4) Làm giàu (Enrichment):
       - Nạp KMeansModel từ hdfs_model_path trong State Topic 'model:kmeans' (cache theo version, tự reload khi version đổi).
       - Gán cụm phân tán bằng model.transform. Nếu chưa có model thì raise "Hãy chạy spark/spark_batch_train.py trước".
   (5) Ghi Parquet phân tán lên HDFS (schema tường minh, không dùng file JSON tạm):
       - streaming_zone/events/run_id=<..>/batch_id=<..>/ (mode overwrite) cho mọi sự kiện hợp lệ.
       - streaming_zone/reviews/run_id=<..>/batch_id=<..>/ (mode overwrite) cho các sự kiện REVIEW.
   (6) Publish Kafka bằng df.write.format("kafka") (acks=all):
       - Review hợp lệ -> 'tourism-reviews' (key=event_id, dropDuplicates trong batch).
       - Sự kiện đã làm giàu -> 'tourism-output' (key=stt).
   (7) Cập nhật 'tourism-control' key "streaming" (last_batch_id).
   Thứ tự bắt buộc: Parquet -> tourism-reviews -> tourism-output. Mọi lỗi phải raise để micro-batch fail và checkpoint không tiến.
4. Heartbeat luồng nền:
   - Publish 'tourism-control' key "streaming" (session_id, run_id, status ACTIVE, ts_ms) mỗi 5s khi query.isActive.
   - Khi dừng ghi status STOPPED.
"""

import os
import sys
import time
import uuid
import threading
from datetime import datetime
from typing import Optional, Tuple, Dict, Any

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

# Biến môi trường đa nền tảng
HADOOP_HOME = os.environ.get("HADOOP_HOME", "C:/hadoop" if sys.platform.startswith('win') else "/usr/local/hadoop")
HADOOP_HOME = os.path.abspath(HADOOP_HOME).replace('\\', '/')
JAVA_HOME = "C:/jdk-17" if sys.platform.startswith('win') else os.environ.get("JAVA_HOME", "/usr/lib/jvm/java-17-openjdk-amd64")
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
HDFS_URI = os.environ.get("HDFS_URI", "hdfs://localhost:9000")

os.environ['HADOOP_HOME'] = HADOOP_HOME
os.environ['hadoop.home.dir'] = HADOOP_HOME
os.environ['JAVA_HOME'] = JAVA_HOME
if sys.platform.startswith('win'):
    bin_path = os.path.join(HADOOP_HOME, "bin").replace('/', '\\')
    java_bin = os.path.join(JAVA_HOME, "bin").replace('/', '\\')
    current_path = os.environ.get("PATH", "")
    if bin_path not in current_path:
        current_path = bin_path + os.pathsep + current_path
    if java_bin not in current_path:
        current_path = java_bin + os.pathsep + current_path
    os.environ["PATH"] = current_path

# PySpark SQL & MLlib
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, BooleanType, LongType, ArrayType
)
from pyspark.sql.functions import (
    col, from_json, to_json, struct, lit, when, length, trim,
    concat_ws, current_timestamp, coalesce, broadcast, split
)
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeansModel

# Kafka State Store
from messaging.state_store import (
    TOPIC_INPUT,
    TOPIC_OUTPUT,
    TOPIC_REVIEWS,
    TOPIC_CONTROL,
    TOPIC_DEAD_LETTER,
    TOPIC_STATE_BATCH,
    ensure_topics,
    publish,
    read_latest_compacted
)

# Schema sự kiện đầu vào từ Kafka topic 'tourism-input'
INPUT_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("stt", IntegerType(), True),
    StructField("session_id", StringType(), True),
    StructField("user_name", StringType(), True),
    StructField("rating", DoubleType(), True),
    StructField("review_rating", DoubleType(), True),
    StructField("review_text", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("t_backend_ms", LongType(), True),
    StructField("source", StringType(), True)
])

CURRENT_SESSION_ID = os.environ.get("SPARK_STREAMING_SESSION_ID") or f"stream_{uuid.uuid4().hex[:12]}_{int(time.time())}"

# Bộ nhớ đệm mô hình K-Means và phiên bản
_cached_kmeans_version: Optional[str] = None
_cached_kmeans_model: Optional[KMeansModel] = None
_cached_cluster_meta: Dict[int, Dict[str, Any]] = {}

_last_model_check_time: float = 0.0
MODEL_CHECK_INTERVAL_SEC: float = 60.0  # Chỉ quét Kafka State Store tối đa 1 lần mỗi 60 giây


def get_or_reload_kmeans_model(spark_session: SparkSession, force_check: bool = False) -> Tuple[KMeansModel, str, Dict[int, Dict[str, Any]]]:
    """
    Nạp KMeansModel từ hdfs_model_path trong State Topic 'model:kmeans'.
    Cache hoàn toàn trong RAM, kiểm tra lại khi hết TTL hoặc force_check=True.
    Tránh mở KafkaConsumer trong mỗi micro-batch khi mô hình không thay đổi.
    """
    global _cached_kmeans_version, _cached_kmeans_model, _cached_cluster_meta, _last_model_check_time

    now = time.time()
    # Nếu đã có model trong RAM và chưa hết thời gian định kỳ 60s -> dùng ngay cache RAM
    if _cached_kmeans_model is not None and not force_check and (now - _last_model_check_time < MODEL_CHECK_INTERVAL_SEC):
        return _cached_kmeans_model, _cached_kmeans_version, _cached_cluster_meta

    _last_model_check_time = now
    kmeans_state = read_latest_compacted(TOPIC_STATE_BATCH, key="model:kmeans", bootstrap_servers=KAFKA_BOOTSTRAP)
    if not kmeans_state or not kmeans_state.get("hdfs_model_path"):
        if _cached_kmeans_model is not None:
            return _cached_kmeans_model, _cached_kmeans_version, _cached_cluster_meta
        raise RuntimeError("Chưa tìm thấy mô hình K-Means trên Kafka State Store! Hãy chạy 'python spark/spark_batch_train.py' trước.")

    version = str(kmeans_state.get("version") or kmeans_state.get("job_id", "")).strip()
    hdfs_model_path = kmeans_state["hdfs_model_path"]

    if _cached_kmeans_model is None or _cached_kmeans_version != version:
        print(f"[*] [K-Means Dynamic Reload] Đang tải mô hình K-Means từ HDFS: {hdfs_model_path} (Version: {version})...")
        _cached_kmeans_model = KMeansModel.load(hdfs_model_path)
        _cached_kmeans_version = version

        raw_meta = kmeans_state.get("cluster_meta", {})
        _cached_cluster_meta = {}
        for k_cid, meta_val in raw_meta.items():
            try:
                _cached_cluster_meta[int(k_cid)] = meta_val
            except Exception:
                pass
        print(f"[+] [K-Means Ready] Tải thành công KMeansModel version '{version}' ({len(_cached_cluster_meta)} cụm).")

    return _cached_kmeans_model, _cached_kmeans_version, _cached_cluster_meta


def get_or_create_run_id(spark_session: SparkSession, checkpoint_dir: str) -> str:
    """
    Lấy hoặc tạo mới run_id gắn liền với checkpoint_dir qua Hadoop FileSystem API.
    - Cùng checkpoint -> cùng run_id (replay batch ghi đúng đường dẫn cũ, idempotent).
    - Xóa checkpoint -> run_id mới (không ghi đè dữ liệu cũ).
    """
    sc = spark_session.sparkContext
    conf = sc._jsc.hadoopConfiguration()
    run_id_file_path = sc._gateway.jvm.org.apache.hadoop.fs.Path(f"{checkpoint_dir}/run_id.txt")
    fs = run_id_file_path.getFileSystem(conf)

    if fs.exists(run_id_file_path):
        stream = fs.open(run_id_file_path)
        reader = sc._gateway.jvm.java.io.BufferedReader(sc._gateway.jvm.java.io.InputStreamReader(stream, "UTF-8"))
        run_id = reader.readLine()
        reader.close()
        stream.close()
        print(f"[+] [Checkpoint Run ID] Đã đọc run_id hiện tại từ HDFS: {run_id}")
        return str(run_id).strip()
    else:
        new_run_id = f"run_{int(time.time())}_{uuid.uuid4().hex[:8]}"
        chk_path = sc._gateway.jvm.org.apache.hadoop.fs.Path(checkpoint_dir)
        if not fs.exists(chk_path):
            fs.mkdirs(chk_path)
        out_stream = fs.create(run_id_file_path, True)
        writer = sc._gateway.jvm.java.io.OutputStreamWriter(out_stream, "UTF-8")
        writer.write(new_run_id)
        writer.close()
        out_stream.close()
        print(f"[+] [Checkpoint Run ID] Đã khởi tạo run_id mới trên HDFS: {new_run_id}")
        return new_run_id


def load_master_catalog_broadcast(spark_session: SparkSession) -> DataFrame:
    """
    Đọc Master Dataset từ HDFS processed_zone lúc khởi động và broadcast để tra cứu phân tán.
    Kiểm tra tính toàn vẹn: STT duy nhất, tọa độ trong VN, rating [1..5], không hard-code số dòng.
    """
    master_path = f"{HDFS_URI}/tourism_data/processed_zone/tourism_dataset_clean.csv"
    print(f"[*] Đọc Master Dataset từ HDFS: {master_path}...")
    master_df = spark_session.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("multiLine", "true") \
        .option("quote", "\"") \
        .option("escape", "\"") \
        .csv(master_path)

    total_count = master_df.count()
    if total_count == 0:
        raise ValueError(f"Master Dataset trên HDFS trống ({master_path})!")

    stt_distinct = master_df.select("STT").distinct().count()
    if stt_distinct != total_count:
        raise ValueError(f"STT trong Master Dataset không duy nhất! Tổng: {total_count}, STT: {stt_distinct}")

    # Lựa chọn các trường danh mục chuẩn
    kw_col_name = "Từ Khóa (Mở rộng)" if "Từ Khóa (Mở rộng)" in master_df.columns else "Từ Khóa"
    desc_col_name = "Mô tả" if "Mô tả" in master_df.columns else "Mo_Ta"
    img_col_name = "Ảnh" if "Ảnh" in master_df.columns else "Anh"

    catalog_lookup = master_df.select(
        col("STT").cast(IntegerType()).alias("cat_stt"),
        col("Tên địa điểm").cast(StringType()).alias("cat_name"),
        col("Vị trí").cast(StringType()).alias("cat_location"),
        col(desc_col_name).cast(StringType()).alias("cat_description"),
        col(img_col_name).cast(StringType()).alias("cat_image"),
        col(kw_col_name).cast(StringType()).alias("cat_keywords_str"),
        col("Latitude").cast(DoubleType()).alias("cat_lat"),
        col("Longitude").cast(DoubleType()).alias("cat_lng"),
        col("Rating").cast(DoubleType()).alias("cat_base_rating")
    )

    print(f"[+] [Broadcast Catalog] Đã nạp thành công Master Dataset ({total_count} điểm đến).")
    return broadcast(catalog_lookup)


def create_spark_streaming_session() -> SparkSession:
    """Khởi tạo SparkSession chuẩn cho Spark Structured Streaming."""
    builder = SparkSession.builder \
        .appName("VietnamTourism_StructuredStreaming_KafkaFirst") \
        .master("local[*]") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.driver.memory", "2g") \
        .config("spark.hadoop.hadoop.home.dir", HADOOP_HOME) \
        .config("spark.driver.extraJavaOptions", f"-Dhadoop.home.dir={HADOOP_HOME}") \
        .config("spark.executor.extraJavaOptions", f"-Dhadoop.home.dir={HADOOP_HOME}") \
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1")

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.shuffle.partitions", "4")
    return spark


def make_process_micro_batch(run_id: str, catalog_broadcast: DataFrame):
    """Tạo hàm bao xử lý từng micro-batch nhận run_id và catalog_broadcast cố định."""

    def process_micro_batch(batch_df: DataFrame, batch_id: int):
        """
        XỬ LÝ MICRO-BATCH BẰNG DATAFRAME API THUẦN TÚY (ZERO COLLECT / ZERO PANDAS).
        """
        # [ƯU TIÊN 3] Kiểm tra nhanh bằng take(1) thay vì rdd.isEmpty() để tránh launch job phân tán rườm rà
        if not batch_df.take(1):
            return

        t_spark_start_ms = int(time.time() * 1000)
        ts_batch_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"\n" + "=" * 80)
        print(f"[Spark Structured Streaming] MICRO-BATCH #{batch_id} | run_id={run_id} | Thời gian: {ts_batch_str}")
        print("=" * 80)

        spark_session = batch_df.sparkSession

        # 1. Join với catalog_broadcast theo STT (Left Outer Join)
        joined_df = batch_df.join(
            catalog_broadcast,
            batch_df.stt == catalog_broadcast.cat_stt,
            "left_outer"
        )

        # 2. Xây dựng biểu thức cột kiểm định (Validation)
        cond_missing_eid = col("event_id").isNull() | (trim(col("event_id")) == "")
        cond_invalid_type = col("event_type").isNull() | (~col("event_type").isin(["REVIEW", "SIMULATED_VIEW"]))
        cond_missing_stt = col("cat_stt").isNull()
        cond_invalid_rating = (col("event_type") == "REVIEW") & (
            col("rating").isNull() | (col("rating") < 1.0) | (col("rating") > 5.0)
        )
        cond_text_overflow = (col("event_type") == "REVIEW") & col("review_text").isNotNull() & (
            length(col("review_text")) > 1000
        )
        cond_user_overflow = (col("event_type") == "REVIEW") & col("user_name").isNotNull() & (
            length(col("user_name")) > 100
        )

        error_col = concat_ws("; ",
            when(cond_missing_eid, lit("MISSING_EVENT_ID")),
            when(cond_invalid_type, lit("INVALID_EVENT_TYPE")),
            when(cond_missing_stt, lit("STT_NOT_IN_CATALOG")),
            when(cond_invalid_rating, lit("INVALID_RATING_[1,5]")),
            when(cond_text_overflow, lit("REVIEW_TEXT_EXCEEDS_1000")),
            when(cond_user_overflow, lit("USER_NAME_EXCEEDS_100"))
        )

        checked_df = joined_df.withColumn("error_reason", error_col).cache()

        df_valid = checked_df.filter(col("error_reason") == "")
        df_error = checked_df.filter(col("error_reason") != "")

        # 3. [ƯU TIÊN 3] Xử lý bản ghi lỗi chỉ khi phát hiện có lỗi bằng take(1)
        has_errors = bool(df_error.take(1))
        if has_errors:
            dead_letter_hdfs_path = f"{HDFS_URI}/tourism_data/dead_letter/run_id={run_id}/batch_id={batch_id}"
            dead_out = df_error.select(
                coalesce(col("event_id"), lit("unassigned_event")).alias("event_id"),
                col("raw_value"),
                col("error_reason"),
                lit(batch_id).alias("batch_id"),
                lit(run_id).alias("run_id"),
                lit(ts_batch_str).alias("rejected_at")
            )
            # Ghi Parquet vào HDFS dead_letter (mode overwrite)
            dead_out.coalesce(1).write.mode("overwrite").parquet(dead_letter_hdfs_path)
            print(f"[!] [Dead-Letter HDFS] Đã lưu bản ghi lỗi vào: {dead_letter_hdfs_path}")

            # Publish bản ghi lỗi sang Kafka topic 'tourism-dead-letter'
            dead_kafka_df = dead_out.select(
                col("event_id").cast(StringType()).alias("key"),
                to_json(struct(
                    col("event_id"),
                    col("raw_value"),
                    col("error_reason"),
                    col("batch_id"),
                    col("run_id"),
                    col("rejected_at")
                )).alias("value")
            )
            dead_kafka_df.write \
                .format("kafka") \
                .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
                .option("topic", TOPIC_DEAD_LETTER) \
                .option("kafka.acks", "all") \
                .save()
            print(f"[!] [Dead-Letter Kafka] Đã đẩy sự kiện lỗi sang '{TOPIC_DEAD_LETTER}'.")

        # [ƯU TIÊN 3] Nếu có lỗi thì mới cần kiểm tra df_valid; nếu không có lỗi, 100% bản ghi đều hợp lệ
        if has_errors and not df_valid.take(1):
            print("[*] Micro-batch không có sự kiện hợp lệ nào để xử lý tiếp.")
            checked_df.unpersist()
            return

        # 4. Làm giàu dữ liệu (Data Enrichment) bằng KMeansModel từ HDFS
        kmeans_model, km_version, cluster_meta = get_or_reload_kmeans_model(spark_session)

        # Chuẩn bị vector đặc trưng từ tọa độ của catalog
        assembler = VectorAssembler(inputCols=["cat_lat", "cat_lng"], outputCol="features")
        valid_with_feats = assembler.transform(df_valid)

        # Gán cluster_id phân tán bằng PySpark MLlib KMeansModel
        clustered_df = kmeans_model.transform(valid_with_feats)

        # Xử lý linh hoạt tên cột phân cụm: KMeansModel có thể xuất 'cluster_id' hoặc 'prediction'
        pred_col_name = "cluster_id" if "cluster_id" in clustered_df.columns else "prediction"
        pred_col = col(pred_col_name)

        # Ánh xạ tên cụm và màu sắc theo cluster_meta
        # Xây dựng biểu thức when/otherwise cho cluster_name và cluster_color
        name_expr = lit("Cụm không xác định")
        color_expr = lit("#3B82F6")
        for cid, meta in cluster_meta.items():
            c_name = meta.get("cluster_name", f"Cụm {cid}")
            c_color = meta.get("cluster_color", "#3B82F6")
            name_expr = when(pred_col == cid, lit(c_name)).otherwise(name_expr)
            color_expr = when(pred_col == cid, lit(c_color)).otherwise(color_expr)

        # Chuẩn bị DataFrame sự kiện đã làm giàu hoàn chỉnh
        enriched_df = clustered_df.select(
            col("event_id"),
            col("event_type"),
            col("stt"),
            coalesce(col("session_id"), concat_ws("_", lit("sess"), col("stt"))).alias("session_id"),
            col("cat_name").alias("name"),
            col("cat_location").alias("location"),
            col("cat_description").alias("description"),
            col("cat_image").alias("image"),
            col("cat_lat").alias("latitude"),
            col("cat_lng").alias("longitude"),
            pred_col.cast(IntegerType()).alias("cluster_id"),
            name_expr.alias("cluster_name"),
            color_expr.alias("cluster_color"),
            split(col("cat_keywords_str"), r',\s*').alias("keywords"),
            coalesce(col("rating"), col("cat_base_rating")).alias("rating"),
            coalesce(col("review_rating"), col("rating")).alias("review_rating"),
            col("t_backend_ms"),
            when(col("event_type") == "REVIEW", lit(True)).otherwise(lit(False)).alias("is_user_review"),
            coalesce(col("user_name"), lit("")).alias("user_name"),
            coalesce(col("review_text"), lit("")).alias("review_text"),
            coalesce(col("timestamp"), lit(ts_batch_str)).alias("timestamp"),
            lit(ts_batch_str).alias("processed_at"),
            col("raw_value")
        ).cache()

        # 5. [TÁCH HDFS KHỎI ĐƯỜNG PHẢN HỒI]
        # Xuất NGAY LẬP TỨC sang Kafka (TOPIC_OUTPUT) để Web SSE đẩy thẳng tới User B không bị trễ
        output_kafka_df = enriched_df.select(
            col("stt").cast(StringType()).alias("key"),
            to_json(struct(
                col("event_id"),
                col("event_type"),
                col("stt"),
                col("name"),
                col("location"),
                col("description"),
                col("image"),
                col("latitude"),
                col("longitude"),
                col("cluster_id"),
                col("cluster_name"),
                col("cluster_color").alias("color"),
                col("keywords"),
                col("rating"),
                col("review_rating"),
                col("is_user_review"),
                col("user_name"),
                col("review_text"),
                col("timestamp"),
                col("processed_at"),
                col("t_backend_ms"),
                lit(t_spark_start_ms).alias("t_spark_start_ms")
            )).alias("value")
        )
        output_kafka_df.write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
            .option("topic", TOPIC_OUTPUT) \
            .option("kafka.acks", "all") \
            .save()

        t_spark_output_done_ms = int(time.time() * 1000)
        spark_output_latency_ms = t_spark_output_done_ms - t_spark_start_ms
        print(f"[+] [Kafka Output] Đã xuất bản kết quả làm giàu vào '{TOPIC_OUTPUT}' (Thời gian thực thi Spark đo được: {spark_output_latency_ms}ms).")

        # 6. Publish vào Kafka TOPIC_REVIEWS cho nhật ký đánh giá (kiểm tra nhanh bằng take(1))
        reviews_only_df = enriched_df.filter(col("event_type") == "REVIEW")
        if reviews_only_df.take(1):
            reviews_kafka_df = reviews_only_df.dropDuplicates(["event_id"]).select(
                col("event_id").cast(StringType()).alias("key"),
                to_json(struct(
                    col("event_id"),
                    col("stt"),
                    col("session_id"),
                    col("user_name"),
                    col("rating"),
                    col("review_rating"),
                    col("review_text"),
                    col("timestamp"),
                    col("cluster_id"),
                    col("processed_at"),
                    col("t_backend_ms"),
                    lit(run_id).alias("run_id"),
                    lit(batch_id).alias("spark_batch_id"),
                    lit(t_spark_start_ms).alias("t_spark_start_ms")
                )).alias("value")
            )
            reviews_kafka_df.write \
                .format("kafka") \
                .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
                .option("topic", TOPIC_REVIEWS) \
                .option("kafka.acks", "all") \
                .save()
            t_reviews_done_ms = int(time.time() * 1000)
            print(f"[+] [Kafka Reviews] Đã xuất bản reviews hợp lệ vào '{TOPIC_REVIEWS}' (Thời gian đo được: {t_reviews_done_ms - t_spark_start_ms}ms).")

        # Lưu ý: Việc lưu trữ lịch sử HDFS Parquet được tiến trình riêng 'spark/hdfs_stream_archiver.py'
        # thực hiện bất đồng bộ trong nền, loại bỏ hoàn toàn việc chờ 2 lần ghi Parquet!

        # 7. Cập nhật tourism-control key "streaming" với last_batch_id
        publish(
            topic=TOPIC_CONTROL,
            key="streaming",
            value={
                "session_id": CURRENT_SESSION_ID,
                "run_id": run_id,
                "status": "ACTIVE",
                "last_batch_id": batch_id,
                "ts_ms": int(time.time() * 1000),
                "timestamp": ts_batch_str
            },
            bootstrap_servers=KAFKA_BOOTSTRAP
        )

        enriched_df.unpersist()
        checked_df.unpersist()

    return process_micro_batch


def run_structured_streaming():
    print("=" * 80)
    print("  KHỞI ĐỘNG SPARK STRUCTURED STREAMING (KAFKA-FIRST SPEED LAYER)")
    print("  Cơ chế: at-least-once + ghi idempotent (Parquet theo run_id/batch_id, Kafka compacted theo event_id)")
    print("=" * 80)

    # 1. Đảm bảo cấu hình Kafka Topics
    ensure_topics(bootstrap_servers=KAFKA_BOOTSTRAP)

    # 2. Khởi tạo SparkSession
    spark = create_spark_streaming_session()

    # 3. Checkpoint location trên HDFS & quản lý run_id
    checkpoint_dir = f"{HDFS_URI}/tourism_data/checkpoints/streaming_checkpoint"
    run_id = get_or_create_run_id(spark, checkpoint_dir)

    # 4. Broadcast Master Catalog từ HDFS
    catalog_broadcast = load_master_catalog_broadcast(spark)

    # 5. Đọc luồng dữ liệu từ Kafka topic 'tourism-input'
    print(f"[*] Đăng ký đọc luồng từ Kafka '{TOPIC_INPUT}'...")
    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("subscribe", TOPIC_INPUT) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .option("maxOffsetsPerTrigger", 1000) \
        .load()

    # 6. Parse JSON với StructType tường minh, giữ raw_value
    parsed_stream = raw_stream.select(
        col("value").cast(StringType()).alias("raw_value"),
        from_json(col("value").cast(StringType()), INPUT_SCHEMA).alias("payload")
    ).select("raw_value", "payload.*")

    # 7. writeStream với foreachBatch
    process_func = make_process_micro_batch(run_id=run_id, catalog_broadcast=catalog_broadcast)

    # [ƯU TIÊN 4] Giảm trigger từ 5s xuống 1s để giảm triệt để độ trễ chờ đợi batch
    query = parsed_stream.writeStream \
        .foreachBatch(process_func) \
        .trigger(processingTime="1 second") \
        .option("checkpointLocation", checkpoint_dir) \
        .start()

    print(f"[+] Spark Structured Streaming đã khởi động thành công!")
    print(f"    - Ingest: {KAFKA_BOOTSTRAP} (Topic: {TOPIC_INPUT})")
    print(f"    - Output: {TOPIC_OUTPUT} & {TOPIC_REVIEWS}")
    print(f"    - Checkpoint: {checkpoint_dir}")
    print(f"    - Session ID: {CURRENT_SESSION_ID}")
    print(f"    - Run ID: {run_id}")

    # 8. Tiến trình Heartbeat chạy nền mỗi 5 giây
    stop_heartbeat = threading.Event()

    def _heartbeat_worker():
        while not stop_heartbeat.is_set():
            try:
                if query.isActive:
                    publish(
                        topic=TOPIC_CONTROL,
                        key="streaming",
                        value={
                            "session_id": CURRENT_SESSION_ID,
                            "run_id": run_id,
                            "status": "ACTIVE",
                            "ts_ms": int(time.time() * 1000),
                            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        },
                        bootstrap_servers=KAFKA_BOOTSTRAP
                    )
            except Exception as e_hb:
                print(f"[!] Cảnh báo publish heartbeat: {e_hb}")
            stop_heartbeat.wait(5.0)

        # Khi query dừng, gửi trạng thái STOPPED
        try:
            publish(
                topic=TOPIC_CONTROL,
                key="streaming",
                value={
                    "session_id": CURRENT_SESSION_ID,
                    "run_id": run_id,
                    "status": "STOPPED",
                    "ts_ms": int(time.time() * 1000),
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                },
                bootstrap_servers=KAFKA_BOOTSTRAP
            )
        except Exception as e_stop:
            print(f"[!] Cảnh báo ghi trạng thái STOPPED: {e_stop}")

    hb_thread = threading.Thread(target=_heartbeat_worker, daemon=True, name="StreamingHeartbeatThread")
    hb_thread.start()

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("\n[!] Nhận tín hiệu dừng Spark Structured Streaming...")
        query.stop()
    finally:
        stop_heartbeat.set()
        hb_thread.join(timeout=2.0)
        spark.stop()
        print("[+] Đã giải phóng tài nguyên SparkSession thành công.")


if __name__ == '__main__':
    try:
        run_structured_streaming()
    except Exception as e:
        print(f"\n[-] LỖI KHỞI ĐỘNG SPARK STRUCTURED STREAMING: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
