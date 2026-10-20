"""
MODULE GỘP FILE NHỎ (COMPACTION) CHO STREAMING ZONE
=============================================================================
Mục đích:
  Đọc các micro-batch nhỏ trong:
    `hdfs://.../tourism_data/streaming_zone/events/run_id=<RUN_ID>/batch_id=<batch_id>/`
    `hdfs://.../tourism_data/streaming_zone/reviews/run_id=<RUN_ID>/batch_id=<batch_id>/`
  Khử trùng lặp theo `event_id`, ghi nén Snappy coalesce(1) mỗi lần chạy vào:
    `hdfs://.../tourism_data/analytics_zone/streaming_events/dt=<YYYY-MM-DD>/run_id=<timestamp>/`
    `hdfs://.../tourism_data/analytics_zone/streaming_reviews/dt=<YYYY-MM-DD>/run_id=<timestamp>/`

Tính năng an toàn & Idempotent:
  1. Hỗ trợ cả 2 vùng dữ liệu streaming: 'events' và 'reviews' (cờ --target [all|events|reviews]).
  2. Phân vùng dt=<YYYY-MM-DD> lấy từ ngày của timestamp sự kiện (fallback về ngày hiện tại nếu thiếu).
  3. Liệt kê danh sách thư mục batch thỏa mãn điều kiện (--older-than-minutes).
  4. Đọc kiểm tra dữ liệu và dropDuplicates(["event_id"]) trên toàn bộ dt=<YYYY-MM-DD>.
  5. Với cờ --delete-source: Chỉ xóa đúng từng thư mục batch trong danh sách đã gộp và đã verify count khớp 100%.
     Tuyệt đối không dùng xóa wildcard.
=============================================================================
"""

import os
import sys
import argparse
import subprocess
from datetime import datetime, timedelta

if sys.platform.startswith('win'):
    try:
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
        sys.stderr.reconfigure(encoding='utf-8', errors='replace')
    except Exception:
        pass

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)

HADOOP_HOME = os.environ.get("HADOOP_HOME", "C:\\hadoop" if sys.platform.startswith('win') else "/usr/local/hadoop")
HDFS_URI = os.environ.get("HDFS_URI", "hdfs://localhost:9000")

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, substring, when, lit


def get_hdfs_cmd():
    hdfs_bin = "hdfs.cmd" if sys.platform.startswith('win') else "hdfs"
    candidate = os.path.join(HADOOP_HOME, "bin", hdfs_bin)
    return candidate if os.path.exists(candidate) else hdfs_bin


def list_batch_directories(spark, src_path, older_than_minutes=10):
    """
    Quét và liệt kê danh sách các thư mục batch_id cụ thể thỏa mãn điều kiện thời gian.
    Trả về: (fs, [danh sách đường dẫn thư mục batch])
    """
    sc = spark.sparkContext
    conf = sc._jsc.hadoopConfiguration()
    Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
    src_hdfs_path = Path(src_path)
    fs = src_hdfs_path.getFileSystem(conf)

    if not fs.exists(src_hdfs_path):
        return fs, []

    # Quét cả cấu trúc mới run_id=*/batch_id=* và cấu trúc cũ batch_id=*
    statuses_new = fs.globStatus(Path(f"{src_path}/run_id=*/batch_id=*"))
    statuses_legacy = fs.globStatus(Path(f"{src_path}/batch_id=*"))

    all_statuses = []
    if statuses_new:
        all_statuses.extend(list(statuses_new))
    if statuses_legacy:
        all_statuses.extend(list(statuses_legacy))

    if not all_statuses:
        return fs, []

    cutoff_ms = (datetime.now() - timedelta(minutes=older_than_minutes)).timestamp() * 1000.0

    eligible_dirs = []
    for s in all_statuses:
        if s.isDirectory():
            dir_str = s.getPath().toString()
            mtime = s.getModificationTime()
            if older_than_minutes <= 0 or mtime <= cutoff_ms:
                eligible_dirs.append(dir_str)

    return fs, eligible_dirs


def compact_subzone(spark, subzone_name, older_than_minutes=10, delete_source=False, target_date=None, compaction_run_id=None):
    """
    Gộp file nhỏ cho 1 subzone ('events' hoặc 'reviews').
    """
    if compaction_run_id is None:
        compaction_run_id = datetime.now().strftime("%Y%m%d_%H%M%S")

    src_path = f"{HDFS_URI}/tourism_data/streaming_zone/{subzone_name}"
    dest_prefix = f"{HDFS_URI}/tourism_data/analytics_zone/streaming_{subzone_name}"

    print(f"\n--- [COMPACTION SUBZONE: {subzone_name.upper()}] ---")
    print(f"[*] Quét thư mục nguồn: {src_path}")
    fs, eligible_batch_dirs = list_batch_directories(spark, src_path, older_than_minutes)

    if not eligible_batch_dirs:
        print(f"[+] Không có batch nào trong '{subzone_name}' cần gộp.")
        return 0

    print(f"[+] Tìm thấy {len(eligible_batch_dirs)} thư mục batch hợp lệ:")
    for bdir in eligible_batch_dirs:
        print(f"    -> {bdir}")

    # Đọc dữ liệu
    df_src = spark.read.parquet(*eligible_batch_dirs)
    raw_count = df_src.count()
    if raw_count == 0:
        print(f"[+] Các thư mục batch trong '{subzone_name}' không chứa bản ghi nào.")
        return 0

    print(f"[+] Tổng số bản ghi nguồn '{subzone_name}': {raw_count}")

    # Xác định dt
    today_str = datetime.now().strftime("%Y-%m-%d")
    if target_date:
        df_with_dt = df_src.withColumn("event_dt", lit(target_date))
    elif "timestamp" in df_src.columns:
        df_with_dt = df_src.withColumn(
            "event_dt",
            when(col("timestamp").rlike("^\\d{4}-\\d{2}-\\d{2}"), substring(col("timestamp"), 1, 10))
            .otherwise(lit(today_str))
        )
    else:
        df_with_dt = df_src.withColumn("event_dt", lit(today_str))

    distinct_dates = [r[0] for r in df_with_dt.select("event_dt").distinct().collect() if r[0]]
    if not distinct_dates:
        distinct_dates = [today_str]

    total_compacted = 0
    for dt_val in distinct_dates:
        df_date_subset = df_with_dt.filter(col("event_dt") == dt_val).drop("event_dt")
        dedup_col = "event_id" if "event_id" in df_date_subset.columns else df_date_subset.columns[0]
        df_date_dedup = df_date_subset.dropDuplicates([dedup_col])
        expected_count = df_date_dedup.count()

        if expected_count == 0:
            continue

        dest_run_path = f"{dest_prefix}/dt={dt_val}/run_id={compaction_run_id}"
        print(f"[*] [dt={dt_val}] Ghi {expected_count} bản ghi vào: {dest_run_path}")
        df_date_dedup.coalesce(1).write.mode("overwrite").parquet(dest_run_path)

        # Verify count
        df_run_verify = spark.read.parquet(dest_run_path)
        actual_count = df_run_verify.count()
        print(f"[+] [dt={dt_val}] Verify Run: Đã ghi {actual_count} bản ghi (kỳ vọng: {expected_count})")
        if actual_count != expected_count:
            raise RuntimeError(f"Lỗi toàn vẹn compaction {subzone_name}: {actual_count} != {expected_count} tại {dest_run_path}")

        total_compacted += expected_count

    print(f"[+] Hoàn tất gộp {total_compacted} bản ghi cho subzone '{subzone_name}'.")

    # Xóa nguồn nếu được yêu cầu
    if delete_source:
        Path = spark.sparkContext._gateway.jvm.org.apache.hadoop.fs.Path
        hdfs_cmd = get_hdfs_cmd()
        print(f"[*] Xóa {len(eligible_batch_dirs)} thư mục batch nguồn trong '{subzone_name}'...")
        deleted = 0
        for bdir in eligible_batch_dirs:
            del_success = False
            try:
                del_success = fs.delete(Path(bdir), True)
            except Exception:
                pass
            if not del_success:
                res = subprocess.run([hdfs_cmd, "dfs", "-rm", "-r", "-skipTrash", bdir], capture_output=True)
                del_success = (res.returncode == 0)
            if del_success:
                deleted += 1
            else:
                raise RuntimeError(f"Không thể xóa thư mục batch nguồn: {bdir}")
        print(f"[+] Đã xóa thành công {deleted}/{len(eligible_batch_dirs)} thư mục batch nguồn trong '{subzone_name}'.")

    return total_compacted


def run_compaction(older_than_minutes=10, delete_source=False, target_date=None, target="all"):
    compaction_run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    cutoff_time = datetime.now() - timedelta(minutes=older_than_minutes)
    cutoff_str = cutoff_time.strftime("%Y-%m-%d %H:%M:%S")

    print("=" * 85)
    print(f"  KHỞI ĐỘNG COMPACTION STREAMING ZONE -> ANALYTICS ZONE")
    print(f"  - Mã phiên gộp (Compaction Run ID): {compaction_run_id}")
    print(f"  - Mục tiêu (--target): {target}")
    print(f"  - Ngưỡng thời gian: cũ hơn {older_than_minutes} phút (trước {cutoff_str})")
    print(f"  - Xóa nguồn sau khi gộp (--delete-source): {delete_source}")
    print("=" * 85)

    spark = SparkSession.builder \
        .appName(f"Tourism_Streaming_Compaction_{compaction_run_id}") \
        .master("local[*]") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    total_records = 0
    try:
        subzones = []
        if target in ("all", "events"):
            subzones.append("events")
        if target in ("all", "reviews"):
            subzones.append("reviews")

        for sz in subzones:
            rec_cnt = compact_subzone(
                spark=spark,
                subzone_name=sz,
                older_than_minutes=older_than_minutes,
                delete_source=delete_source,
                target_date=target_date,
                compaction_run_id=compaction_run_id
            )
            total_records += rec_cnt

        print("=" * 85)
        print(f"  COMPACTION HOÀN TẤT THÀNH CÔNG: TỔNG {total_records} BẢN GHI ĐÃ ĐƯỢC GỘP.")
        print("=" * 85)
        return total_records
    finally:
        spark.stop()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Spark Compaction Job cho Streaming Zone HDFS")
    parser.add_argument("--older-than-minutes", type=int, default=10, help="Chỉ gộp các batch cũ hơn N phút (mặc định: 10)")
    parser.add_argument("--delete-source", action="store_true", help="Xóa thư mục batch nguồn sau khi đã verify count khớp 100%")
    parser.add_argument("--target-date", type=str, default=None, help="Ngày partition đích dt=YYYY-MM-DD")
    parser.add_argument("--target", type=str, default="all", choices=["all", "events", "reviews"], help="Đối tượng gộp: all, events hoặc reviews")

    args = parser.parse_args()
    try:
        run_compaction(
            older_than_minutes=args.older_than_minutes,
            delete_source=args.delete_source,
            target_date=args.target_date,
            target=args.target
        )
        sys.exit(0)
    except Exception as e:
        print(f"\n[-] LỖI COMPACTION: {e}")
        sys.exit(1)

