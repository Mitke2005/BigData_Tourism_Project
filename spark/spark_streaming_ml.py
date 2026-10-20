"""
spark/spark_streaming_ml.py
===========================
FILE TƯƠNG THÍCH NGƯỢC (BACKWARD COMPATIBILITY)
LƯU Ý KIẾN TRÚC & PHÂN TÁCH LUỒNG:
- Thuật toán MLlib cốt lõi (SparkTourismCoreEngine) đã được tách sang `spark/tourism_engine.py` (Phase 1).
- Vòng lặp KafkaConsumer legacy đã bị XÓA HOÀN TOÀN theo đặc tả Phase 1 để tránh xử lý trùng lặp và xung đột Speed Layer.
- Luồng Speed Layer chính thức duy nhất là `spark/spark_structured_streaming.py` (Apache Spark Structured Streaming).
- Luồng Batch Layer chính thức là `spark/spark_batch_train.py`.
"""

import os
import sys

if sys.platform.startswith('win'):
    try:
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
        sys.stderr.reconfigure(encoding='utf-8', errors='replace')
    except Exception:
        pass

# Thêm đường dẫn project vào sys.path
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)

# Re-export các thành phần từ spark/tourism_engine.py
from spark.tourism_engine import (
    SparkTourismCoreEngine,
    CLUSTER_PALETTE,
    clean_keyword_str,
    extract_clean_keywords,
    haversine_distance,
    sparse_cosine_similarity
)

TourismMLPipeline = SparkTourismCoreEngine

if __name__ == '__main__':
    print("[*] spark_streaming_ml.py: Vòng lặp KafkaConsumer legacy đã được loại bỏ.")
    print("[*] Thuật toán PySpark MLlib hiện tại nằm tại `spark/tourism_engine.py`.")
    print("[*] Để chạy huấn luyện batch: python spark/spark_batch_train.py")
    print("[*] Để chạy Speed Layer streaming: python spark/spark_structured_streaming.py")
