"""
hadoop/batch_sync_pipeline.py
=============================
WRAPPER MỎNG ĐIỀU PHỐI BATCH SYNC PIPELINE (LAMBDA ARCHITECTURE)
Quy trình:
1. Đảm bảo Master Dataset đã có trên HDFS (hdfs://localhost:9000/tourism_data/processed_zone/tourism_dataset_clean.csv).
   Nếu CHƯA có: đẩy lên HDFS (tuyệt đối không ghi đè nếu đã tồn tại).
2. Gọi thực thi Job Batch Spark MLlib chính thức (`spark/spark_batch_train.py`).
3. Toàn bộ cơ chế SQLite và ghi sửa Master CSV đã bị LOẠI BỎ TRIỆT ĐỂ theo đặc tả Kafka-First.
"""

import os
import sys
import socket
import argparse
import subprocess
from typing import Dict, Any, Optional

if sys.platform.startswith('win'):
    try:
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
        sys.stderr.reconfigure(encoding='utf-8', errors='replace')
    except Exception:
        pass

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

DATA_DIR = os.path.join(BASE_DIR, "data")
LOCAL_MASTER_CSV = os.path.join(DATA_DIR, "tourism_dataset_clean.csv")
HDFS_TARGET_DIR = "/tourism_data/processed_zone"
HDFS_TARGET_FILE = "/tourism_data/processed_zone/tourism_dataset_clean.csv"


def is_hdfs_port_open(port: int = 9000) -> bool:
    """Kiểm tra cổng giao tiếp của Hadoop NameNode."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(1.0)
    try:
        s.connect(('localhost', port))
        s.close()
        return True
    except Exception:
        return False


def ensure_master_on_hdfs(hadoop_home: Optional[str] = None) -> bool:
    """
    Đẩy Master Dataset lên HDFS nếu CHƯA có (hỗ trợ cả WSL và Windows).
    """
    try:
        from hadoop.hdfs_sync import upload_dataset_to_hdfs
        upload_dataset_to_hdfs()
        return True
    except Exception as e:
        print(f"[-] [HDFS Sync Error] {e}")
        raise e


def run_batch_sync(job_id: Optional[str] = None, regions: Optional[str] = None) -> Dict[str, Any]:
    """
    Điều phối chu trình Batch Sync: Đảm bảo HDFS -> Chạy spark_batch_train.py.
    """
    print("=" * 80)
    print("  BATCH SYNC PIPELINE (WRAPPER MỎNG): ĐIỀU PHỐI ĐỒNG BỘ VÀ HUẤN LUYỆN BATCH")
    print("=" * 80)

    # 1. Kiểm tra kết nối HDFS
    if not is_hdfs_port_open(9000):
        print("[-] [Batch Sync Lỗi] Hadoop HDFS (cổng 9000) đang ngoại tuyến! Vui lòng khởi động NameNode.")
        return {"success": False, "error": "Hadoop HDFS offline (port 9000)"}

    # 2. Đảm bảo Master Dataset có trên HDFS (không ghi đè)
    try:
        ensure_master_on_hdfs()
    except Exception as e:
        print(f"[-] [Batch Sync Lỗi] Không thể tải Master Dataset lên HDFS: {e}")
        return {"success": False, "error": str(e)}

    # 3. Kích hoạt Spark Batch Training
    print("[*] Chuyển tiếp quyền điều khiển sang `spark/spark_batch_train.py`...")
    from spark.spark_batch_train import run_batch_training

    parsed_regions = [r.strip() for r in regions.split(",") if r.strip()] if regions else None
    result = run_batch_training(job_id=job_id, regions=parsed_regions)
    return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Wrapper mỏng điều phối Batch Sync Pipeline")
    parser.add_argument("--job-id", type=str, default=None, help="Mã phiên huấn luyện (Job ID)")
    parser.add_argument("--regions", type=str, default=None, help="Danh sách vùng/tỉnh thành cần lập lịch trình")

    args = parser.parse_args()
    res = run_batch_sync(job_id=args.job_id, regions=args.regions)
    sys.exit(0 if res.get("success") else 1)
