import os
import sys
import time
import pandas as pd
import numpy as np

if sys.platform.startswith('win'):
    try:
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
        sys.stderr.reconfigure(encoding='utf-8', errors='replace')
    except:
        pass

import builtins

# Biến môi trường đa nền tảng cho Hadoop & winutils trên Windows
HADOOP_HOME = os.environ.get("HADOOP_HOME", "C:/hadoop" if sys.platform.startswith('win') else "/usr/local/hadoop")
HADOOP_HOME = os.path.abspath(HADOOP_HOME).replace('\\', '/')
os.environ['HADOOP_HOME'] = HADOOP_HOME
os.environ['hadoop.home.dir'] = HADOOP_HOME
JAVA_HOME = "C:/jdk-17" if sys.platform.startswith('win') else os.environ.get("JAVA_HOME", "/usr/lib/jvm/java-17-openjdk-amd64")
os.environ['JAVA_HOME'] = JAVA_HOME
os.environ['HADOOP_USER_NAME'] = os.environ.get('HADOOP_USER_NAME', 'mitke')

if sys.platform.startswith('win'):
    bin_path = os.path.join(HADOOP_HOME, "bin").replace('/', '\\')
    java_bin = os.path.join(JAVA_HOME, "bin").replace('/', '\\')
    current_path = os.environ.get("PATH", "")
    if bin_path not in current_path:
        current_path = bin_path + os.pathsep + current_path
    if java_bin not in current_path:
        current_path = java_bin + os.pathsep + current_path
    os.environ["PATH"] = current_path

# Thư viện PySpark
try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, when, avg, count, desc, min as spark_min, max as spark_max, round as spark_round
    HAS_PYSPARK = True
except ImportError:
    HAS_PYSPARK = False

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_PATH = os.path.join(BASE_DIR, "data", "tourism_dataset_clean.csv")
PARQUET_PATH = os.path.join(BASE_DIR, "data", "hdfs_lake", "parquet", "tourism_data.parquet")
HDFS_URI = os.environ.get("HDFS_URI", "hdfs://localhost:9000")
HDFS_PROCESSED_PATH = f"{HDFS_URI}/tourism_data/processed_zone/tourism_dataset_clean.csv"

def run_spark_sql():
    print("="*75)
    print("  TRUY VẤN PHÂN TÍCH KHÁM PHÁ VỚI SPARK SQL TRỰC TIẾP TỪ HDFS")
    print("="*75)

    if HAS_PYSPARK:
        spark = SparkSession.builder \
            .appName("Tourism_Spark_SQL_Analytics") \
            .master("local[*]") \
            .config("spark.driver.memory", "2g") \
            .config("spark.driver.host", "127.0.0.1") \
            .config("spark.driver.bindAddress", "127.0.0.1") \
            .config("spark.hadoop.hadoop.home.dir", HADOOP_HOME) \
            .config("spark.driver.extraJavaOptions", f"-Dhadoop.home.dir={HADOOP_HOME}") \
            .config("spark.executor.extraJavaOptions", f"-Dhadoop.home.dir={HADOOP_HOME}") \
            .config("spark.sql.shuffle.partitions", "4") \
            .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")

        # Thử đọc trực tiếp từ HDFS theo yêu cầu đề tài
        read_path = HDFS_PROCESSED_PATH
        print(f"[*] Thử đọc dữ liệu trực tiếp từ HDFS: {read_path}...")
        try:
            df_spark = spark.read \
                .option("header", "true") \
                .option("multiLine", "true") \
                .option("quote", "\"") \
                .option("escape", "\"") \
                .option("inferSchema", "true") \
                .csv(read_path)
            total_records = df_spark.count()
            print(f"[+] [HDFS Native] Đọc thành công từ HDFS: {total_records} bản ghi!")
        except Exception as e_hdfs:
            print(f"[!] HDFS chưa sẵn sàng hoặc không kết nối được ({e_hdfs}). Chuyển sang fallback file cục bộ...")
            read_path = DATA_PATH
            df_spark = spark.read \
                .option("header", "true") \
                .option("multiLine", "true") \
                .option("quote", "\"") \
                .option("escape", "\"") \
                .option("inferSchema", "true") \
                .csv(read_path)
            total_records = df_spark.count()
            print(f"[+] [Local Fallback] Đọc thành công từ file cục bộ: {total_records} bản ghi!")

        df_spark.createOrReplaceTempView("tourism_poi")

        # 1. Top 10 Tỉnh/Thành phố có nhiều điểm du lịch nhất
        print("\n--- 1. TOP 10 TỈNH/THÀNH PHỐ CÓ NHIỀU ĐIỂM DU LỊCH NHẤT ---")
        q1 = """
        SELECT `Vị trí` AS TinhThanh, COUNT(*) AS SoLuongDiem, ROUND(AVG(Rating), 2) AS DiemDanhGiaTrungBinh
        FROM tourism_poi
        GROUP BY `Vị trí`
        ORDER BY SoLuongDiem DESC
        LIMIT 10
        """
        spark.sql(q1).show(truncate=False)

        # 2. Phân bố địa điểm theo 3 miền địa lý dựa vào tọa độ Latitude
        print("\n--- 2. PHÂN BỐ ĐỊA ĐIỂM THEO 3 MIỀN ĐỊA LÝ ---")
        q2 = """
        SELECT 
            CASE 
                WHEN Latitude >= 20.0 THEN 'Miền Bắc'
                WHEN Latitude >= 14.0 AND Latitude < 20.0 THEN 'Miền Trung'
                ELSE 'Miền Nam'
            END AS VungMien,
            COUNT(*) AS SoDiemDen,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS TyLePhanTram,
            ROUND(AVG(Rating), 2) AS RatingTB
        FROM tourism_poi
        GROUP BY 
            CASE 
                WHEN Latitude >= 20.0 THEN 'Miền Bắc'
                WHEN Latitude >= 14.0 AND Latitude < 20.0 THEN 'Miền Trung'
                ELSE 'Miền Nam'
            END
        ORDER BY SoDiemDen DESC
        """
        spark.sql(q2).show(truncate=False)

        # 3. Phân loại theo thang điểm đánh giá
        print("\n--- 3. PHÂN BỐ MẬT ĐỘ THEO THANG ĐIỂM ĐÁNH GIÁ ---")
        q3 = """
        SELECT 
            CASE 
                WHEN Rating >= 4.8 THEN 'Xuất sắc (>= 4.8 sao)'
                WHEN Rating >= 4.5 THEN 'Rất tốt (4.5 - 4.7 sao)'
                WHEN Rating >= 4.0 THEN 'Tốt (4.0 - 4.4 sao)'
                ELSE 'Trung bình (< 4.0 sao)'
            END AS PhanHang,
            COUNT(*) AS SoLuong,
            ROUND(AVG(Rating), 2) AS RatingTB
        FROM tourism_poi
        GROUP BY 
            CASE 
                WHEN Rating >= 4.8 THEN 'Xuất sắc (>= 4.8 sao)'
                WHEN Rating >= 4.5 THEN 'Rất tốt (4.5 - 4.7 sao)'
                WHEN Rating >= 4.0 THEN 'Tốt (4.0 - 4.4 sao)'
                ELSE 'Trung bình (< 4.0 sao)'
            END
        ORDER BY SoLuong DESC
        """
        spark.sql(q3).show(truncate=False)

        # 4. Thử nghiệm đo đạc hiệu năng: So sánh CSV vs Parquet (Mục 3.3.4 trong đồ án)
        print("\n--- 4. ĐO ĐẠC HIỆU NĂNG: SO SÁNH TRUY VẤN CSV VÀ PARQUET THỰC TẾ ---")
        t0 = time.time()
        c_csv = spark.read.option("header", "true").option("multiLine", "true").csv(DATA_PATH).filter("Rating > 4.5").count()
        time_csv = time.time() - t0

        # Đảm bảo file Parquet thật tồn tại bằng cách ghi từ Spark DataFrame nếu chưa có
        if not os.path.exists(PARQUET_PATH):
            print("[*] Đang khởi tạo dữ liệu Parquet chuẩn nén Snappy từ DataFrame...")
            df_spark.write.mode("overwrite").parquet(PARQUET_PATH)

        t1 = time.time()
        c_parquet = spark.read.parquet(PARQUET_PATH).filter("Rating > 4.5").count()
        time_parquet = time.time() - t1

        speedup = (time_csv / max(time_parquet, 0.001))
        print(f"[*] Thời gian quét truy vấn CSV:     {time_csv*1000:.2f} ms (Bản ghi: {c_csv})")
        print(f"[*] Thời gian quét truy vấn Parquet: {time_parquet*1000:.2f} ms (Bản ghi: {c_parquet})")
        print(f"[+] Cơ chế nén cột Snappy & Predicate Pushdown giúp tối ưu I/O thực tế (Tốc độ: {speedup:.1f}x)!")

        spark.stop()

    else:
        # Fallback bằng Pandas / DuckDB nếu PySpark chưa set SPARK_HOME
        print("[*] Đang thực thi phân tích SQL khám phá dữ liệu...")
        df = pd.read_csv(DATA_PATH)
        print("Top 10 Tỉnh/Thành phố:")
        print(df['Vị trí'].value_counts().head(10))

if __name__ == '__main__':
    run_spark_sql()
