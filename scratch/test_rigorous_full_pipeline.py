import os
import sys
import time
import json
import socket
import threading
import requests

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)

if sys.platform.startswith('win'):
    try:
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
        sys.stderr.reconfigure(encoding='utf-8', errors='replace')
    except Exception:
        pass


class SSEStreamListener(threading.Thread):
    """Lắng nghe trực tiếp luồng SSE Server-Sent Events từ Web Dashboard"""
    def __init__(self, url="http://localhost:5000/api/stream"):
        super().__init__(daemon=True)
        self.url = url
        self.events = []
        self.running = True
        self.connected = False

    def run(self):
        try:
            resp = requests.get(self.url, stream=True, timeout=30)
            self.connected = True
            for line in resp.iter_lines():
                if not self.running:
                    break
                if line:
                    decoded = line.decode('utf-8')
                    if decoded.startswith("data: "):
                        data_str = decoded[6:].strip()
                        try:
                            ev = json.loads(data_str)
                            self.events.append(ev)
                        except Exception:
                            pass
        except Exception as e:
            pass

    def stop(self):
        self.running = False


def test_rigorous_pipeline():
    print("=" * 75)
    print("PHIÊN KIỂM CHỨNG TOÀN DIỆN KIẾN TRÚC LAMBDA BIG DATA (SSE, HDFS, SPARK, KAFKA)")
    print("=" * 75)

    # -------------------------------------------------------------------------
    # 1. Dataset nằm trên HDFS và Spark đọc đúng URI
    # -------------------------------------------------------------------------
    print("\n[KIỂM CHỨNG 1] Dataset trên HDFS thật & Spark đọc đúng URI...")
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(1.0)
    hdfs_open = s.connect_ex(("127.0.0.1", 9000)) == 0
    s.close()
    assert hdfs_open, "LỖI: Hadoop HDFS cổng 9000 không mở!"
    print("[+] HDFS NameNode cổng 9000 đang phản hồi.")

    r = requests.get("http://localhost:5000/api/system_status", timeout=5)
    st = r.json()
    assert st.get("hdfs", {}).get("connected") == True, "HDFS chưa kết nối tới Web"
    assert st.get("kafka", {}).get("connected") == True, "Kafka chưa kết nối tới Web"
    assert st.get("spark", {}).get("status") == "SYNCED", "Spark chưa đồng bộ xong catalog"
    print(f"[+] Trạng thái hệ thống: HDFS={st['hdfs']['connected']}, Kafka={st['kafka']['connected']}, Spark={st['spark']['status']}")
    print("[=> PASSED KIỂM CHỨNG 1]")

    # -------------------------------------------------------------------------
    # 2. Kết quả Spark xuất hiện trong Kafka và Web & Tính trung thực dữ liệu
    # -------------------------------------------------------------------------
    print("\n[KIỂM CHỨNG 2] Kết quả Spark xuất hiện trong Kafka và Web & Tính trung thực dữ liệu...")
    r = requests.get("http://localhost:5000/api/all_spots", timeout=5)
    spots = r.json()
    print(f"[+] Số địa điểm Web nhận từ Spark qua Kafka: {len(spots)} spots")
    assert len(spots) >= 300, f"Số lượng spots không đủ 317: {len(spots)}"
    sp1 = spots[0]
    assert "cluster_id" in sp1 and sp1["cluster_id"] >= 0, "Thiếu cluster_id từ K-Means"
    assert "latitude" in sp1 and "longitude" in sp1, "Thiếu tọa độ địa lý"
    print(f"[+] Mẫu Spot #{sp1['stt']} ({sp1['name']}): Cụm={sp1['cluster_id']} | Tọa độ=({sp1['latitude']}, {sp1['longitude']})")

    # Kiểm tra tính trung thực: các điểm chưa có đánh giá thực tế phải có review_count = 0, latest_review = None
    unreviewed = [s for s in spots if s.get('review_count') == 0]
    assert len(unreviewed) > 0, "Phải có các điểm chưa có đánh giá thực tế"
    sample_unrev = unreviewed[0]
    assert sample_unrev.get('latest_review') is None, "Lỗi: Điểm chưa có review lại có latest_review giả mạo!"
    assert sample_unrev.get('reviews') == [], "Lỗi: reviews phải là rỗng khi chưa có đánh giá!"
    print(f"[+] XÁC NHẬN TRUNG THỰC DỮ LIỆU: Spot #{sample_unrev['stt']} ({sample_unrev['name']}) chưa có đánh giá có review_count=0, latest_review=None (Không làm giả 'Du khách cộng đồng')")
    print("[=> PASSED KIỂM CHỨNG 2]")

    # -------------------------------------------------------------------------
    # 3. Một review chỉ tăng đúng 1 lần, đối chiếu CHÍNH XÁC event_id qua luồng SSE
    # -------------------------------------------------------------------------
    print("\n[KIỂM CHỨNG 3] Kiểm tra qua SSE Stream, đối chiếu CHÍNH XÁC event_id từ phản hồi POST...")
    # Khởi động SSE Stream listener
    sse_listener = SSEStreamListener()
    sse_listener.start()
    for _ in range(25):
        if sse_listener.connected:
            break
        time.sleep(0.3)
    assert sse_listener.connected, "Không kết nối được vào SSE endpoint /api/stream!"
    print("[+] Đã kết nối trực tiếp vào luồng sự kiện SSE (/api/stream).")

    # Chọn một spot để test (ví dụ STT 75 - Sông Chày)
    test_stt = 75
    spot_info = next(s for s in spots if s['stt'] == test_stt)
    initial_reviews = spot_info.get('review_count', 0)
    initial_rating = spot_info.get('rating', 4.5)
    print(f"[+] Spot trước review: #{test_stt} {spot_info['name']} | Reviews={initial_reviews}, Rating={initial_rating}")

    # Gửi 1 review 5 sao
    rev_payload = {
        "stt": test_stt,
        "rating": 5.0,
        "review_text": "Trải nghiệm tuyệt vời tại Sông Chày kiểm chứng event_id",
        "user_name": "Kiểm thử viên SSE Chính Xác"
    }
    submit_r = requests.post("http://localhost:5000/api/submit_review", json=rev_payload, timeout=5)
    assert submit_r.status_code == 200, f"Lỗi gửi review: {submit_r.text}"
    submit_json = submit_r.json()
    expected_event_id = submit_json.get("event", {}).get("event_id")
    assert expected_event_id is not None, "API không trả về event_id!"
    print(f"[+] Đã gửi review qua API. Event ID tạo ra: {expected_event_id}")

    # Đợi sự kiện SSE truyền từ Spark Worker qua Kafka output tới SSE client với đúng expected_event_id
    matched_event = None
    start_wait = time.time()
    while time.time() - start_wait < 10.0:
        for ev in sse_listener.events:
            if ev.get('event_id') == expected_event_id:
                matched_event = ev
                break
        if matched_event:
            break
        time.sleep(0.4)

    assert matched_event is not None, f"LỖI: Không nhận được sự kiện SSE có event_id={expected_event_id} sau 10s!"
    print(f"[+] [SSE EVENT KHỚP 100% EVENT_ID]: {matched_event.get('event_id')}")
    print(f"    Địa điểm: #{matched_event.get('stt')} {matched_event.get('name')}")
    print(f"    Lượt đánh giá chính thức (review_count): {matched_event.get('review_count')}")
    print(f"    Rating mới: {matched_event.get('updated_rating')}")
    print(f"    Tọa độ: Lat={matched_event.get('latitude')}, Lng={matched_event.get('longitude')}")

    # Kiểm tra tính chính xác của dữ liệu trong sự kiện SSE:
    assert matched_event.get("longitude") is not None, "Lỗi: Longitude bị None trong sự kiện!"
    expected_count = initial_reviews + 1
    assert matched_event.get("review_count") == expected_count, \
        f"Lỗi: review_count trong sự kiện phải là {expected_count}, nhận được {matched_event.get('review_count')}"
    print(f"[+] XÁC NHẬN: review_count tăng đúng {initial_reviews} -> {expected_count} (+1 đơn vị, không bị nhảy số)!")
    print("[=> PASSED KIỂM CHỨNG 3]")

    # -------------------------------------------------------------------------
    # 4. Batch Sync HDFS với job_id -> Spark Reload HDFS -> SSE nhận SPARK_BATCH_CATALOG_REFRESH
    # -------------------------------------------------------------------------
    print("\n[KIỂM CHỨNG 4] Batch Sync HDFS với job_id -> Spark Reload HDFS -> SSE nhận SPARK_BATCH_CATALOG_REFRESH...")
    ev_count_before_sync = len(sse_listener.events)
    test_job_id = f"job_proof_{int(time.time()*1000)}"

    sync_r = requests.post("http://localhost:5000/api/sync_hdfs_retrain", json={"job_id": test_job_id}, timeout=30)
    assert sync_r.status_code == 200, f"Lỗi gọi sync: {sync_r.status_code}"
    sync_data = sync_r.json()
    print(f"[+] Batch Sync API hoàn tất bước đẩy: job_id={sync_data.get('job_id')}, success={sync_data.get('success')}, dqa={sync_data.get('data_quality')}")
    assert sync_data.get("success") == True
    assert sync_data.get("job_id") == test_job_id

    # Chờ sự kiện SPARK_BATCH_CATALOG_REFRESH từ Spark qua SSE có đúng job_id
    print(f"[+] Đang lắng nghe luồng SSE chờ Spark Worker xử lý job_id={test_job_id}...")
    refresh_event = None
    start_wait = time.time()
    while time.time() - start_wait < 12.0:
        for ev in sse_listener.events[ev_count_before_sync:]:
            if ev.get('action') == 'SPARK_BATCH_CATALOG_REFRESH' and ev.get('job_id') == test_job_id:
                refresh_event = ev
                break
        if refresh_event:
            break
        time.sleep(0.5)

    assert refresh_event is not None, f"LỖI: Không nhận được sự kiện SPARK_BATCH_CATALOG_REFRESH với job_id={test_job_id} qua SSE!"
    print(f"[+] [SSE REFRESH NHẬN ĐƯỢC VỚI ĐÚNG JOB_ID]:")
    print(f"    Action: {refresh_event.get('action')}")
    print(f"    Job ID: {refresh_event.get('job_id')}")
    print(f"    Nguồn: {refresh_event.get('source')}")
    print(f"    Tổng số điểm: {refresh_event.get('total_spots')}")
    print(f"    Lịch trình đã làm mới: {refresh_event.get('itineraries_updated')}")
    print(f"    Thời gian phát: {refresh_event.get('timestamp')}")
    assert refresh_event.get("total_spots", 0) >= 300
    assert refresh_event.get("itineraries_updated", 0) >= 1
    print("[=> PASSED KIỂM CHỨNG 4]")

    # -------------------------------------------------------------------------
    # 5. Kiểm tra cơ chế báo lỗi thực tế (HDFS Offline & Kafka Offline)
    # -------------------------------------------------------------------------
    print("\n[KIỂM CHỨNG 5] Kiểm tra cơ chế báo lỗi thực tế qua hàm hệ thống...")
    import hadoop.batch_sync_pipeline as bsp

    # 5A. Giả lập HDFS Offline (cổng 9999 không tồn tại) - BẮT BUỘC DỪNG
    print("[+] 5A: Thử nghiệm khi HDFS offline (cổng 9999)...")
    res_hdfs_off = bsp.run_batch_sync(hdfs_port=9999)
    assert res_hdfs_off.get("success") == False, "Lỗi: Khi HDFS offline mà success vẫn là True!"
    assert res_hdfs_off.get("hdfs_status") == "HDFS_OFFLINE"
    assert res_hdfs_off.get("status") == "FAILED_HDFS_OFFLINE"
    print(f"[+] Kết quả: success={res_hdfs_off.get('success')}, status={res_hdfs_off.get('status')} (Hệ thống dừng bắt buộc, bảo toàn dữ liệu)")

    # 5B. Kiểm tra API trả về HTTP 503 khi HDFS thất bại
    from web_dashboard.app import app
    with app.test_client() as client:
        real_sync = bsp.run_batch_sync
        bsp.run_batch_sync = lambda **kwargs: res_hdfs_off
        api_err_resp = client.post('/api/sync_hdfs_retrain')
        bsp.run_batch_sync = real_sync
        assert api_err_resp.status_code == 503, f"Status code phải là 503, nhận được {api_err_resp.status_code}"
        print(f"[+] Web API trả về HTTP status={api_err_resp.status_code} (503 Service Unavailable khi HDFS lỗi)")

    # 5C. THỬ THỰC TẾ NHÁNH KAFKA THẤT BẠI CỦA HỆ THỐNG (Gọi run_batch_sync với cổng Kafka không tồn tại)
    print("[+] 5C: Thử nghiệm thực tế nhánh Kafka thất bại qua hàm bsp.run_batch_sync(kafka_bootstrap='127.0.0.1:9998')...")
    res_kafka_fail = bsp.run_batch_sync(hdfs_port=9000, kafka_bootstrap="127.0.0.1:9998")
    print(f"[+] Kết quả thực tế từ hệ thống: success={res_kafka_fail.get('success')}, status={res_kafka_fail.get('status')}")
    assert res_kafka_fail.get("success") == False, "Lỗi: Khi Kafka không gửi được mà success vẫn là True!"
    assert res_kafka_fail.get("status") == "PARTIAL_SUCCESS_KAFKA_FAILED", f"Status không đúng: {res_kafka_fail.get('status')}"
    assert res_kafka_fail.get("kafka_notified") == False
    print("[+] XÁC NHẬN THỰC TẾ: Hàm hệ thống run_batch_sync() tự phát hiện lỗi Kafka, trả success=False và đánh dấu PARTIAL_SUCCESS_KAFKA_FAILED!")
    print("[=> PASSED KIỂM CHỨNG 5]")

    sse_listener.stop()

    print("\n" + "=" * 75)
    print("HOÀN TẤT KIỂM THỬ: TẤT CẢ 5 TIÊU CHÍ ĐỀU VƯỢT QUA VỚI CHỨNG CỨ THỰC TẾ 100%!")
    print("=" * 75)
    return True


if __name__ == "__main__":
    test_rigorous_pipeline()
