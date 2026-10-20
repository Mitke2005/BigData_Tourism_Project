import os
import sys
import shutil

if sys.platform.startswith('win'):
    try:
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
        sys.stderr.reconfigure(encoding='utf-8', errors='replace')
    except Exception:
        pass

os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["JAVA_HOME"] = "C:/jdk-17"

from pyspark.sql import SparkSession

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CSV_PATH = os.path.join(BASE_DIR, "data", "tourism_dataset_clean.csv")
PARQUET_DIR = os.path.join(BASE_DIR, "data", "hdfs_lake", "parquet")
ANALYTICS_PARQUET = os.path.join(BASE_DIR, "data", "hdfs_lake", "analytics_zone", "tourism_lake_spark.parquet")

def main():
    print("=" * 80)
    print("XÓA 5.047 TỆP PARQUET NHỎ & TÁI XUẤT BẢN BẰNG PYSPARK COALESCE(1)")
    print("=" * 80)

    # 1. Đếm và xóa các tệp nhỏ rải rác trong data/hdfs_lake/parquet
    if os.path.exists(PARQUET_DIR):
        small_files = [f for f in os.listdir(PARQUET_DIR) if f.startswith("part_") and f.endswith(".parquet")]
        print(f"[*] Phát hiện {len(small_files)} tệp phân mảnh nhỏ trong '{PARQUET_DIR}'.")
        for f in small_files:
            try:
                os.remove(os.path.join(PARQUET_DIR, f))
            except Exception as e:
                pass
        remaining = len(os.listdir(PARQUET_DIR))
        print(f"[+] Đã xóa sạch {len(small_files)} tệp nhỏ phân mảnh! Số tệp còn lại trong thư mục: {remaining}")

    # 2. Khởi tạo SparkSession
    print("[*] Khởi tạo Apache SparkSession...")
    spark = SparkSession.builder \
        .appName("CleanParquetExport") \
        .master("local[*]") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()

    # 3. Đọc dữ liệu từ CSV bằng Spark
    print(f"[*] Đọc Master CSV từ '{CSV_PATH}' bằng PySpark...")
    df = spark.read.option("header", "true") \
        .option("multiLine", "true") \
        .option("quote", "\"") \
        .option("escape", "\"") \
        .csv(CSV_PATH)

    total_records = df.count()
    print(f"[+] Đã nạp thành công {total_records} bản ghi vào Spark DataFrame.")

    # 4. Ghi Parquet bằng một lệnh Spark với coalesce(1) chuẩn Big Data (chống Small File Problem)
    # Ghi vào Data Lake cục bộ
    spark_parquet_out = os.path.join(BASE_DIR, "data", "hdfs_lake", "parquet", "tourism_lake_spark")
    print(f"[*] Ghi Parquet vào Data Lake với coalesce(1) tại '{spark_parquet_out}'...")
    df.coalesce(1).write.mode("overwrite").parquet(spark_parquet_out)

    # Ghi đồng thời lên Hadoop HDFS analytics_zone
    hdfs_parquet_uri = "hdfs://localhost:9000/tourism_data/analytics_zone/tourism_dataset.parquet"
    print(f"[*] Ghi Parquet trực tiếp lên HDFS tại '{hdfs_parquet_uri}'...")
    try:
        df.coalesce(1).write.mode("overwrite").parquet(hdfs_parquet_uri)
        print(f"[+] Đã ghi thành công Parquet lên HDFS Analytics Zone: {hdfs_parquet_uri}")
    except Exception as e:
        print(f"[!] Cảnh báo ghi HDFS (Spark Native): {e}")

    # Tạo tệp parquet đơn khối tourism_lake.parquet trong data/hdfs_lake/parquet để tương thích các module
    lake_single = os.path.join(BASE_DIR, "data", "hdfs_lake", "parquet", "tourism_lake.parquet")
    spark_part_files = [f for f in os.listdir(spark_parquet_out) if f.startswith("part-") and f.endswith(".parquet")]
    if spark_part_files:
        src = os.path.join(spark_parquet_out, spark_part_files[0])
        shutil.copy2(src, lake_single)
        size_kb = round(os.path.getsize(lake_single) / 1024, 2)
        print(f"[+] Đã xuất bản tệp Parquet đơn khối chuẩn: '{lake_single}' ({size_kb} KB)")

    spark.stop()
    print("=" * 80)
    print("HOÀN TẤT DỌN DẸP & XUẤT BẢN PARQUET CHUẨN SPARK!")
    print("=" * 80)

if __name__ == '__main__':
    main()
