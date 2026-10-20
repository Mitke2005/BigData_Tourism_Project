"""
scratch/test_streaming_unit.py
==============================
UNIT TEST KIỂM TRA LOGIC VALIDATION & DEAD-LETTER CHO SPARK STRUCTURED STREAMING (PHASE 2)
Sử dụng Spark SQL native (không dùng Python RDD pickle để tương thích hoàn toàn Python 3.14).
"""

import os
import sys

if sys.platform.startswith('win'):
    try:
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
        sys.stderr.reconfigure(encoding='utf-8', errors='replace')
    except Exception:
        pass

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, when, length, trim, concat_ws, broadcast
)


def test_validation_logic():
    print("=" * 80)
    print("  KIỂM ĐỊNH BIỂU THỨC CỘT VALIDATION & DEAD-LETTER CỦA STREAMING (PHASE 2)")
    print("=" * 80)

    spark = SparkSession.builder \
        .appName("StreamingValidationUnitTest") \
        .master("local[1]") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # 1. Tạo catalog qua Spark SQL native (tránh bug recursion pickle của Python 3.14)
    cat_sql = """
    SELECT 1 AS cat_stt, 'Bãi Dài' AS cat_name, 'Khánh Hòa' AS cat_location, 12.05 AS cat_lat, 109.20 AS cat_lng, 4.8 AS cat_base_rating
    UNION ALL SELECT 2, 'Tháp Bà Ponagar', 'Khánh Hòa', 12.23, 109.20, 4.5
    UNION ALL SELECT 3, 'Thành cổ Diên Khánh', 'Khánh Hòa', 12.24, 109.19, 4.5
    """
    catalog_df = spark.sql(cat_sql)
    cat_broadcast = broadcast(catalog_df)

    # 2. Tạo input qua Spark SQL native
    # Tạo chuỗi văn bản dài 1005 ký tự và user name 105 ký tự
    long_text = "A" * 1005
    long_user = "U" * 105

    input_sql = f"""
    SELECT 'ev_01' AS event_id, 'REVIEW' AS event_type, 1 AS stt, 'sess_1' AS session_id, 'Alice' AS user_name, 4.5 AS rating, 'Tuyệt vời' AS review_text, '2026-10-10 12:00:00' AS timestamp, 'web' AS source
    UNION ALL SELECT 'ev_02', 'SIMULATED_VIEW', 2, 'sess_2', '', CAST(NULL AS DOUBLE), '', '2026-10-10 12:01:00', 'producer'
    UNION ALL SELECT '', 'REVIEW', 1, 'sess_1', 'Bob', 4.0, 'Ổn', '2026-10-10 12:02:00', 'web'
    UNION ALL SELECT 'ev_04', 'HACK_COMMAND', 1, 'sess_1', 'Eve', 5.0, 'Test', '2026-10-10 12:03:00', 'web'
    UNION ALL SELECT 'ev_05', 'REVIEW', 999, 'sess_1', 'Charlie', 5.0, 'Địa điểm lạ', '2026-10-10 12:04:00', 'web'
    UNION ALL SELECT 'ev_06', 'REVIEW', 2, 'sess_1', 'Dan', 9.0, 'Quá sao', '2026-10-10 12:05:00', 'web'
    UNION ALL SELECT 'ev_07', 'REVIEW', 3, 'sess_1', 'Frank', 4.0, '{long_text}', '2026-10-10 12:06:00', 'web'
    UNION ALL SELECT 'ev_08', 'REVIEW', 3, 'sess_1', '{long_user}', 4.0, 'Tên dài', '2026-10-10 12:07:00', 'web'
    """
    input_df = spark.sql(input_sql)

    # 3. Thực thi logic biểu thức cột như trong spark_structured_streaming.py
    joined_df = input_df.join(
        cat_broadcast,
        input_df.stt == cat_broadcast.cat_stt,
        "left_outer"
    )

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

    checked_df = joined_df.withColumn("error_reason", error_col)

    df_valid = checked_df.filter(col("error_reason") == "")
    df_error = checked_df.filter(col("error_reason") != "")

    valid_count = df_valid.count()
    error_count = df_error.count()

    print(f"[*] Số sự kiện hợp lệ: {valid_count} (Kỳ vọng: 2)")
    print(f"[*] Số sự kiện lỗi (Dead-Letter): {error_count} (Kỳ vọng: 6)")

    assert valid_count == 2, f"valid_count={valid_count} != 2"
    assert error_count == 6, f"error_count={error_count} != 6"

    error_rows = df_error.select("event_id", "stt", "event_type", "error_reason").collect()
    for row in error_rows:
        print(f"    [Dead-Letter] Event: '{row.event_id}' | STT: {row.stt} | Type: '{row.event_type}' => Reason: {row.error_reason}")

    spark.stop()
    print("=" * 80)
    print("  [PASS] 100% CÁC BIỂU THỨC CỘT VALIDATION ĐÃ VƯỢT QUA KIỂM ĐỊNH!")
    print("=" * 80)


if __name__ == '__main__':
    test_validation_logic()
