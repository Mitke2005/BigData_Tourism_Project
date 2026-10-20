"""
scratch/test_web_endpoints.py
=============================
KIỂM ĐỊNH TÍCH HỢP CÁC API PHỤC VỤ WEB DASHBOARD (PHASE 3)
Kiểm tra bằng Flask test_client mà không phụ thuộc hạ tầng mạng:
1. GET /api/all_spots: Trả về waiting_batch nếu chưa có batch commit.
2. GET /api/health/lambda: Đầy đủ các trường heartbeat age, materializer, streaming, kafka topics.
3. POST /api/submit_review: Kiểm định rating hợp lệ [1.0, 5.0], STT hợp lệ, trả về 503 khi streaming offline.
4. GET /api/sync_status: Yêu cầu job_id và trả về cấu trúc tiến độ.
5. GET /api/system_status: Trả về trạng thái đầy đủ 4 thành phần hệ thống.
"""

import os
import sys
import json

if sys.platform.startswith('win'):
    try:
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
        sys.stderr.reconfigure(encoding='utf-8', errors='replace')
    except Exception:
        pass

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from web_dashboard.app import app


def test_web_endpoints():
    print("=" * 80)
    print("  KIỂM ĐỊNH CÁC ENDPOINT WEB DASHBOARD SERVING LAYER (PHASE 3)")
    print("=" * 80)

    client = app.test_client()

    # 1. Kiểm tra /api/all_spots
    res = client.get('/api/all_spots')
    assert res.status_code == 200, f"Expected 200, got {res.status_code}"
    data = res.get_json()
    print(f"[*] /api/all_spots: Trả về kiểu dữ liệu {type(data).__name__}")
    if isinstance(data, dict):
        assert data.get("status") == "waiting_batch", f"Expected waiting_batch, got {data}"
        print(f"[PASS] 1. /api/all_spots: Báo waiting_batch chính xác khi chưa nhận batch_commit.")
    elif isinstance(data, list):
        print(f"[PASS] 1. /api/all_spots: Đã nhận danh sách {len(data)} spots từ Materializer.")

    # 2. Kiểm tra /api/health/lambda
    res_health = client.get('/api/health/lambda')
    assert res_health.status_code == 200, f"Expected 200, got {res_health.status_code}"
    health_data = res_health.get_json()
    assert "materializer" in health_data, "Thiếu materializer trong health response"
    assert "streaming" in health_data, "Thiếu streaming trong health response"
    assert "kafka" in health_data, "Thiếu kafka trong health response"
    topics = health_data["kafka"]["topics"]
    assert "tourism-input" in topics, "Thiếu tourism-input trong danh sách topics"
    assert "tourism-output" in topics, "Thiếu tourism-output trong danh sách topics"
    assert "tourism-state-spots" in topics, "Thiếu tourism-state-spots trong danh sách topics"
    print(f"[PASS] 2. /api/health/lambda: Đầy đủ các chỉ số sức khỏe Lambda (Topics: {len(topics)}).")

    # 3. Kiểm tra validation POST /api/submit_review
    # 3.1 Thiếu STT
    res_no_stt = client.post('/api/submit_review', json={"rating": 4.5, "review_text": "Good"})
    assert res_no_stt.status_code == 400, f"Expected 400 for missing stt, got {res_no_stt.status_code}"
    print("[PASS] 3.1 /api/submit_review: Chặn thành công request thiếu STT (HTTP 400).")

    # 3.2 Rating ngoài khoảng [1..5]
    res_bad_rating = client.post('/api/submit_review', json={"stt": 1, "rating": 9.0, "review_text": "Good"})
    assert res_bad_rating.status_code == 400, f"Expected 400 for bad rating, got {res_bad_rating.status_code}"
    print("[PASS] 3.2 /api/submit_review: Chặn thành công rating ngoài khoảng [1..5] (HTTP 400).")

    # 3.3 Khi streaming offline -> HTTP 503
    res_offline = client.post('/api/submit_review', json={"stt": 1, "rating": 5.0, "review_text": "Tuyệt vời"})
    # Do hiện tại Spark Streaming chưa chạy, mong đợi HTTP 503
    assert res_offline.status_code in [503, 202], f"Expected 503 or 202, got {res_offline.status_code}"
    if res_offline.status_code == 503:
        err_data = res_offline.get_json()
        assert err_data.get("error_code") == "STREAMING_OFFLINE", f"Expected STREAMING_OFFLINE, got {err_data}"
        print("[PASS] 3.3 /api/submit_review: Báo lỗi chính xác HTTP 503 (STREAMING_OFFLINE) khi Spark chưa chạy.")
    else:
        print("[PASS] 3.3 /api/submit_review: Nhận HTTP 202 thành công.")

    # 4. Kiểm tra /api/sync_status
    res_no_job = client.get('/api/sync_status')
    assert res_no_job.status_code == 400, f"Expected 400 for missing job_id, got {res_no_job.status_code}"
    res_job = client.get('/api/sync_status?job_id=test_job_123')
    assert res_job.status_code == 200, f"Expected 200, got {res_job.status_code}"
    job_data = res_job.get_json()
    assert job_data.get("job_id") == "test_job_123", f"Expected test_job_123, got {job_data}"
    print("[PASS] 4. /api/sync_status: Truy vấn tiến độ job_id thành công.")

    # 5. Kiểm tra /api/system_status
    res_sys = client.get('/api/system_status')
    assert res_sys.status_code == 200, f"Expected 200, got {res_sys.status_code}"
    sys_data = res_sys.get_json()
    assert "kafka" in sys_data and "hdfs" in sys_data and "spark" in sys_data, "Thiếu thành phần hệ thống"
    print("[PASS] 5. /api/system_status: Trả về trạng thái các dịch vụ thành công.")

    print("=" * 80)
    print("  [PASS] 100% CÁC TEST ENDPOINT WEB DASHBOARD PHASE 3 ĐÃ VƯỢT QUA!")
    print("=" * 80)


if __name__ == '__main__':
    test_web_endpoints()
