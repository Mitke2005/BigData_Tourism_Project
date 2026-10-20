"""
scratch/test_batch_publish.py
=============================
KIỂM CHỨNG TOÀN DIỆN PHASE 1: BATCH TRAINING -> KAFKA STATE STORE PUBLISH -> MATERIALIZER
Mục tiêu kiểm định:
1. Master Dataset SHA-256 không bị thay đổi (Tính bất biến tuyệt đối).
2. Chạy job batch spark_batch_train.py với job_id cụ thể.
3. Sử dụng KafkaMaterializer đọc lại từ các State Topics:
   - Có bản ghi 'batch_commit' với status = COMMITTED và job_id tương ứng.
   - Số lượng spot trong Materializer == Số dòng trong Master Dataset.
   - Có model:kmeans (đủ centers, k, hdfs_model_path, version).
   - Có ít nhất một itinerary (ví dụ itinerary:Khánh Hòa:1).
   - Master dataset SHA-256 sau khi chạy KHÔNG ĐỔI so với ban đầu.
"""

import os
import sys
import time
import socket
import hashlib
import pandas as pd

if sys.platform.startswith('win'):
    try:
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
        sys.stderr.reconfigure(encoding='utf-8', errors='replace')
    except Exception:
        pass

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

MASTER_CSV_PATH = os.path.join(BASE_DIR, "data", "tourism_dataset_clean.csv")
MASTER_SHA_PATH = os.path.join(BASE_DIR, "data", "MASTER_DATASET.sha256")


def compute_file_sha256(filepath: str) -> str:
    """Tính toán mã băm SHA-256 của tệp."""
    h = hashlib.sha256()
    with open(filepath, "rb") as f:
        while chunk := f.read(65536):
            h.update(chunk)
    return h.hexdigest()


def is_port_open(port: int, host: str = "localhost") -> bool:
    """Kiểm tra trạng thái cổng TCP."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(1.0)
    try:
        s.connect((host, port))
        s.close()
        return True
    except Exception:
        return False


def test_batch_publish_pipeline():
    print("=" * 80)
    print("  BẮT ĐẦU KIỂM ĐỊNH PHASE 1: BATCH TRAINING & KAFKA STATE STORE PUBLISH")
    print("=" * 80)

    # 1. Kiểm tra SHA-256 Master Dataset trước khi chạy
    if not os.path.exists(MASTER_CSV_PATH):
        print(f"[-] Không tìm thấy tệp Master Dataset: {MASTER_CSV_PATH}")
        sys.exit(1)

    initial_sha = compute_file_sha256(MASTER_CSV_PATH)
    print(f"[*] Master Dataset SHA-256 ban đầu: {initial_sha}")
    if os.path.exists(MASTER_SHA_PATH):
        with open(MASTER_SHA_PATH, "r", encoding="utf-8") as f:
            expected_sha = f.read().strip()
        if initial_sha != expected_sha:
            print(f"[!] Cảnh báo: SHA hiện tại ({initial_sha}) khác expected SHA ({expected_sha})!")

    df_master = pd.read_csv(MASTER_CSV_PATH, encoding='utf-8-sig')
    master_count = len(df_master)
    print(f"[*] Số dòng Master Dataset: {master_count}")

    # 2. Kiểm tra dịch vụ hạ tầng
    kafka_online = is_port_open(9092)
    hdfs_online = is_port_open(9000)

    print(f"[*] Kiểm tra hạ tầng:")
    print(f"    - Kafka Broker (cổng 9092): {'ONLINE' if kafka_online else 'OFFLINE'}")
    print(f"    - Hadoop HDFS (cổng 9000):   {'ONLINE' if hdfs_online else 'OFFLINE'}")

    if not kafka_online:
        print("\n[!] DỊCH VỤ KAFKA ĐANG OFFLINE!")
        print("[!] Không thể chạy kiểm chứng trực tiếp nếu Kafka chưa khởi động.")
        print("[!] Lệnh khởi động Kafka: D:\\BigData\\kafka\\bin\\windows\\kafka-server-start.bat D:\\BigData\\kafka\\config\\server.properties")
        sys.exit(2)

    if not hdfs_online:
        print("\n[!] DỊCH VỤ HADOOP HDFS ĐANG OFFLINE!")
        print("[!] Không thể chạy kiểm chứng trực tiếp nếu Hadoop NameNode chưa mở cổng 9000.")
        print("[!] Lệnh khởi động HDFS: start-dfs.cmd")
        sys.exit(2)

    # 3. Chạy Batch Training
    job_id = f"test_batch_{int(time.time())}"
    print(f"\n[*] Bắt đầu thực thi Batch Training với job_id='{job_id}'...")

    from spark.spark_batch_train import run_batch_training
    batch_res = run_batch_training(
        job_id=job_id,
        regions=['Khánh Hòa', 'Hà Nội', 'Đà Nẵng']
    )

    if not batch_res.get("success"):
        print(f"[-] Batch training thất bại: {batch_res}")
        sys.exit(1)

    print(f"[+] Batch training hoàn thành thành công: {batch_res}")

    # 4. Sử dụng KafkaMaterializer để đọc lại State Store từ Kafka
    print("\n[*] Khởi tạo KafkaMaterializer để đọc lại State Topics...")
    from serving.kafka_materializer import KafkaMaterializer
    mat = KafkaMaterializer()
    mat.start()

    print("[*] Chờ Materializer đồng bộ từ Kafka...")
    mat.wait_until_ready(timeout=20.0)

    # Chờ tối đa 15s để commit thăng cấp
    deadline = time.time() + 15.0
    commit_found = False
    while time.time() < deadline:
        commit_rec = mat.get_batch_commit()
        if commit_rec and commit_rec.get("job_id") == job_id:
            commit_found = True
            break
        time.sleep(0.5)

    commit_rec = mat.get_batch_commit()
    mat.stop()

    # 5. Kiểm tra các điều kiện nghiệm thu
    print("\n" + "=" * 80)
    print("  KẾT QUẢ NGHIỆM THU CHI TIẾT (ASSERTIONS):")
    print("=" * 80)

    # Assertion 1: batch_commit tồn tại & đúng job_id
    assert commit_found, f"Không tìm thấy batch_commit với job_id='{job_id}' trong Materializer!"
    assert commit_rec.get("status") == "COMMITTED", f"Trạng thái commit không phải COMMITTED: {commit_rec.get('status')}"
    print(f"[PASS] 1. batch_commit hợp lệ: status=COMMITTED, job_id={job_id}")

    # Assertion 2: Số spot trong Materializer == Số dòng Master
    spot_views = mat.get_lambda_spot_views()
    print(f"[+] Số spot đọc được từ Materializer: {len(spot_views)}")
    assert len(spot_views) == master_count, f"Số spot trong Materializer ({len(spot_views)}) != Master count ({master_count})"
    print(f"[PASS] 2. Số lượng spots ({len(spot_views)}) khớp 100% với Master Dataset ({master_count}).")

    # Assertion 3: model:kmeans tồn tại trong Materializer
    kmeans_model = mat.get_batch_model("kmeans")
    assert kmeans_model is not None, "Không tìm thấy model:kmeans trong Materializer!"
    assert len(kmeans_model.get("centers", [])) == 5, f"Số tâm cụm K-Means không phải 5: {len(kmeans_model.get('centers', []))}"
    assert kmeans_model.get("hdfs_model_path"), "Thiếu trường hdfs_model_path trong model:kmeans!"
    print(f"[PASS] 3. model:kmeans tồn tại đầy đủ: k={kmeans_model.get('k')}, hdfs_model_path={kmeans_model.get('hdfs_model_path')}")

    # Assertion 4: Itinerary tồn tại trong Materializer
    itin_kh = mat.get_batch_itinerary("Khánh Hòa", 1)
    assert itin_kh is not None and len(itin_kh) > 0, "Không tìm thấy lịch trình Khánh Hòa ngày 1 trong Materializer!"
    print(f"[PASS] 4. Lịch trình hợp lệ: tìm thấy {len(itin_kh)} chặng cho Khánh Hòa 1 ngày.")

    # Assertion 5: Master Dataset SHA-256 sau khi chạy KHÔNG ĐỔI
    after_sha = compute_file_sha256(MASTER_CSV_PATH)
    assert after_sha == initial_sha, f"Master Dataset đã bị sửa đổi! Ban đầu: {initial_sha}, Sau khi chạy: {after_sha}"
    print(f"[PASS] 5. Master Dataset hoàn toàn bất biến (SHA-256 không đổi: {after_sha}).")

    print("=" * 80)
    print("  TẤT CẢ 5/5 ĐIỀU KIỆN KIỂM CHỨNG PHASE 1 ĐỀU [PASS]!")
    print("=" * 80)


if __name__ == '__main__':
    test_batch_publish_pipeline()
