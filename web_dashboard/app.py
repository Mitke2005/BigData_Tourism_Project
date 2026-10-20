"""
web_dashboard/app.py
====================
WEB DASHBOARD SERVING LAYER (KAFKA-FIRST LAMBDA ARCHITECTURE)
Nguyên tắc phục vụ:
1. MATERIALIZED VIEW TRONG RAM:
   - Khởi động KafkaMaterializer đọc Compacted Topics từ đầu tới cuối.
   - Toàn bộ trạng thái Batch Spots, Batch Models, Itineraries, Real-Time Reviews
     và Control State được lưu trong RAM được bảo vệ bởi threading.RLock.
   - Hoàn toàn KHÔNG sử dụng SQLite, không có bất kỳ kết nối DB nào.
2. CÔNG THỨC HỢP NHẤT LAMBDA VIEW:
   - Gọi compute_spot_lambda_view() từ serving/lambda_view.py:
     display_rating = round((base_rating*5 + total_sum) / (5 + total_count), 2).
   - Khử trùng lặp theo event_id; review thuộc synced_event_ids không bị tính 2 lần.
3. TIẾP NHẬN ĐÁNH GIÁ (POST /api/submit_review):
   - Kiểm định tính hợp lệ -> Publish trực tiếp vào topic 'tourism-input' (acks='all', key=session_id).
   - Web KHÔNG tự ghi vào bất kỳ kho hay bộ nhớ cục bộ nào.
   - Trả về HTTP 202 Accepted. Nếu Kafka hoặc Spark Streaming ngoại tuyến (mất nhịp tim >25s) thì trả HTTP 503.
4. SERVER-SENT EVENTS (SSE - GET /api/stream):
   - Luồng nền đọc 'tourism-output' (Consumer Group riêng) -> Khử trùng theo event_id.
   - Với sự kiện review: lấy điểm hợp nhất mới nhất từ KafkaMaterializer RAM rồi phát SSE.
5. ĐỒNG BỘ BATCH ON-DEMAND (POST /api/sync_hdfs_retrain & GET /api/sync_status):
   - Khóa đồng bộ đơn tiến trình, chạy batch job nền với job_id riêng, trả 202.
   - Truy vấn tiến độ qua /api/sync_status từ topic 'tourism-control'.
6. GIÁM SÁT HỆ THỐNG (GET /api/health/lambda & /api/system_status).
"""

import os
import sys
import json
import time
import copy
import uuid
import socket
import threading
import subprocess
from collections import deque
from queue import Queue
from datetime import datetime
from typing import Dict, Any, List, Optional

import selectors
try:
    _orig_fileobj_to_fd = selectors._fileobj_to_fd
    def _safe_fileobj_to_fd(fileobj):
        try:
            return _orig_fileobj_to_fd(fileobj)
        except ValueError as e:
            if "-1" in str(e):
                return -1
            raise
    selectors._fileobj_to_fd = _safe_fileobj_to_fd
except Exception:
    pass

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

from flask import Flask, render_template, Response, jsonify, request
from kafka import KafkaConsumer, TopicPartition

from messaging.state_store import (
    TOPIC_INPUT,
    TOPIC_OUTPUT,
    TOPIC_REVIEWS,
    TOPIC_STATE_SPOTS,
    TOPIC_STATE_BATCH,
    TOPIC_CONTROL,
    TOPIC_DEAD_LETTER,
    DEFAULT_BOOTSTRAP_SERVERS,
    publish
)
from serving.kafka_materializer import get_materializer

app = Flask(__name__)
app.config['TEMPLATES_AUTO_RELOAD'] = True
app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 0

# Khởi tạo KafkaMaterializer singleton trong RAM
materializer = get_materializer()
print("[*] Đang chờ KafkaMaterializer nạp toàn bộ snapshot ban đầu từ Kafka State Topics...")
materializer.wait_until_ready(timeout=10.0)

# Danh sách subscriber lắng nghe SSE
subscribers: List[Queue] = []
subscribers_lock = threading.Lock()
latest_events = deque(maxlen=50)

# Bộ đệm khử trùng lặp sự kiện SSE
seen_sse_event_ids = deque(maxlen=2000)
seen_sse_set = set()
seen_sse_lock = threading.Lock()


def mark_and_check_seen_sse(event_id: str) -> bool:
    """
    Kiểm tra và ghi nhận event_id vào bộ đệm khử trùng lặp SSE (thread-safe).
    Trả về True nếu event_id là MỚI (chưa từng thấy).
    Trả về False nếu event_id ĐÃ TỒN TẠI (trùng lặp).
    """
    if not event_id:
        return True
    with seen_sse_lock:
        if event_id in seen_sse_set:
            return False
        if len(seen_sse_event_ids) >= 2000:
            old_id = seen_sse_event_ids.popleft()
            seen_sse_set.discard(old_id)
        seen_sse_event_ids.append(event_id)
        seen_sse_set.add(event_id)
        return True

# Khóa điều phối chạy batch retrain đơn tiến trình
_batch_sync_lock = threading.Lock()
_active_sync_job_id: Optional[str] = None
_active_sync_process: Optional[subprocess.Popen] = None


def broadcast_event(event: Dict[str, Any]):
    """Phát sự kiện tới tất cả các kết nối SSE đang mở."""
    with subscribers_lock:
        for q in list(subscribers):
            try:
                q.put_nowait(event)
            except Exception:
                pass


# =============================================================================
# TIẾN TRÌNH NỀN LẮNG NGHE KAFKA 'tourism-output' ĐỂ PHÁT SSE
# =============================================================================
def sse_output_listener():
    """Lắng nghe sự kiện từ topic 'tourism-output' và đẩy xuống trình duyệt qua SSE."""
    print("[*] Web Server: Khởi tạo tiến trình nền lắng nghe 'tourism-output' cho SSE...", flush=True)
    last_read_offsets: Dict[TopicPartition, int] = {}

    while True:
        # Kiểm tra nhanh cổng kết nối trước khi tạo consumer để tránh block khi Kafka ngoại tuyến
        s_probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s_probe.settimeout(0.3)
        kafka_is_up = (s_probe.connect_ex(('localhost', 9092)) == 0)
        s_probe.close()
        if not kafka_is_up:
            time.sleep(2.0)
            continue

        consumer = None
        try:
            consumer = KafkaConsumer(
                bootstrap_servers=DEFAULT_BOOTSTRAP_SERVERS,
                value_deserializer=lambda m: (json.loads(m.decode('utf-8')) if m else None),
                api_version=(2, 8, 1),
                consumer_timeout_ms=1000
            )
            # Gán trực tiếp tất cả partition của topic TOPIC_OUTPUT (tránh coordinator rebalance trên Windows)
            partitions = consumer.partitions_for_topic(TOPIC_OUTPUT)
            if not partitions:
                consumer.close()
                time.sleep(1.0)
                continue

            tps = [TopicPartition(TOPIC_OUTPUT, p) for p in partitions]
            consumer.assign(tps)

            # Khôi phục offset: đọc tiếp từ offset đã lưu hoặc nạp 5 sự kiện ban đầu
            for tp in tps:
                try:
                    if tp in last_read_offsets:
                        consumer.seek(tp, last_read_offsets[tp] + 1)
                    else:
                        end_off = consumer.end_offsets([tp]).get(tp, 0)
                        consumer.seek(tp, max(0, end_off - 5))
                except Exception:
                    pass

            print(f"[+] Web Server đã gán {len(tps)} partitions cho topic '{TOPIC_OUTPUT}' và sẵn sàng phát SSE!", flush=True)

            while True:
                msg_batch = consumer.poll(timeout_ms=500)
                if not msg_batch:
                    continue

                for tp, messages in msg_batch.items():
                    for msg in messages:
                        last_read_offsets[tp] = msg.offset
                        event = msg.value
                        if not isinstance(event, dict):
                            continue

                        ev_id = str(event.get("event_id") or "").strip()
                        # Khử trùng lặp theo event_id
                        if ev_id and not mark_and_check_seen_sse(ev_id):
                            continue

                        action = event.get("action", "")
                        is_review = bool(
                            event.get("is_user_review") or
                            action == "NEW_COMMUNITY_REVIEW" or
                            event.get("event_type") == "REVIEW" or
                            event.get("new_review")
                        )

                        # Tạo bản sao độc lập (deepcopy) cho gói tin phát SSE để không làm biến dạng review gốc trong RAM
                        sse_event = copy.deepcopy(event)
                        sse_event["t_web_sse_ms"] = int(time.time() * 1000)

                        orig_review_rating = float(event.get("review_rating") or event.get("rating") or 5.0)
                        sse_event["review_rating"] = orig_review_rating

                        # Nếu là sự kiện review: đảm bảo RAM Materializer đã nhận đúng review trước khi tính điểm SSE
                        if is_review:
                            stt = int(event.get("stt", 0))
                            if stt > 0:
                                rev_to_ingest = copy.deepcopy(event)
                                rev_to_ingest["rating"] = orig_review_rating
                                rev_to_ingest["review_rating"] = orig_review_rating
                                if ev_id:
                                    if not materializer.has_review_event(ev_id):
                                        materializer.ingest_live_review(rev_to_ingest)

                                spot_lambda = materializer.get_spot_lambda_view(stt)
                                if spot_lambda:
                                    sse_event["rating"] = spot_lambda["rating"]
                                    sse_event["updated_rating"] = spot_lambda["rating"]
                                    sse_event["review_count"] = spot_lambda["review_count"]
                                    sse_event["cluster_name"] = spot_lambda.get("cluster_name", event.get("cluster_name", ""))
                                    sse_event["color"] = spot_lambda.get("cluster_color", event.get("color", "#2563EB"))

                        latest_events.append(sse_event)
                        broadcast_event(sse_event)

        except Exception as e:
            if getattr(sys, "is_finalizing", lambda: False)():
                break
            print(f"[-] Lỗi trong sse_output_listener: {e}", flush=True)
            if consumer:
                try:
                    consumer.close()
                except Exception:
                    pass
            time.sleep(2.0)


# Khởi chạy luồng SSE Listener ngầm
sse_thread = threading.Thread(target=sse_output_listener, daemon=True, name="WebSSEListenerThread")
sse_thread.start()


# =============================================================================
# CÁC ROUTE PHỤC VỤ GIAO DIỆN & API
# =============================================================================

@app.route('/')
def index():
    return render_template('index.html')


@app.route('/api/stream')
def stream_events():
    """Endpoint SSE truyền dữ liệu thời gian thực tới trình duyệt."""
    client_queue = Queue(maxsize=100)
    with subscribers_lock:
        subscribers.append(client_queue)

    def event_stream():
        try:
            # Gửi vài sự kiện gần nhất cho client mới kết nối
            for ev in list(latest_events)[-5:]:
                yield f"data: {json.dumps(ev, ensure_ascii=False)}\n\n"

            while True:
                try:
                    event = client_queue.get(timeout=2.0)
                    yield f"data: {json.dumps(event, ensure_ascii=False)}\n\n"
                except Exception:
                    # Gửi heartbeat giữ kết nối
                    yield ": heartbeat\n\n"
        finally:
            with subscribers_lock:
                if client_queue in subscribers:
                    subscribers.remove(client_queue)

    return Response(event_stream(), mimetype="text/event-stream")


@app.route('/api/recent_stream_events')
def get_recent_stream_events():
    """
    Trả về danh sách sự kiện gần nhất từ Spark Streaming (trong RAM latest_events)
    để giao diện nạp ngay tức thì khi tải trang mà không bị chờ lâu.
    """
    events = list(latest_events)
    return jsonify({
        "status": "success",
        "events": events[-15:] if events else []
    })


@app.route('/api/all_spots')
def get_all_spots():
    """
    Trả về toàn bộ danh mục địa điểm hợp nhất từ KafkaMaterializer RAM.
    Nếu chưa có batch_commit, trả thông báo yêu cầu chạy batch training.
    """
    if not materializer.has_batch_commit():
        return jsonify({
            "status": "waiting_batch",
            "message": "Chạy spark_batch_train.py"
        }), 200

    spots = materializer.get_lambda_spot_views()
    return jsonify(spots)


@app.route('/api/spot_reviews/<int:stt>')
def get_spot_reviews_api(stt):
    """
    Trả về chi tiết địa điểm và danh sách nhận xét từ Materialized View trong RAM.
    """
    res = materializer.get_spot_reviews(stt=stt, limit=50)
    if not res or not res.get("spot"):
        return jsonify({"status": "not_found", "message": f"Không tìm thấy địa điểm #{stt}"}), 404

    return jsonify({
        "status": "success",
        "spot": res.get("spot"),
        "reviews": res.get("reviews", []),
        "total_reviews": res.get("total_reviews", 0)
    })


@app.route('/api/submit_review', methods=['POST'])
def submit_customer_review():
    """
    Tiếp nhận đánh giá từ người dùng qua Web:
    1. Kiểm định tính hợp lệ đầu vào.
    2. Kiểm tra trạng thái Kafka Broker và Spark Streaming (nhịp tim <= 25s).
    3. Publish trực tiếp vào topic 'tourism-input' với acks='all' và key=session_id.
    4. Web KHÔNG ghi bất kỳ kho nào; trả về HTTP 202 Accepted.
    """
    try:
        data = request.json or {}

        # 1. Kiểm tra tham số STT
        stt_raw = data.get('stt')
        if stt_raw is None:
            return jsonify({"status": "error", "message": "Thiếu mã định danh địa điểm (STT)!"}), 400
        try:
            stt = int(stt_raw)
        except (ValueError, TypeError):
            return jsonify({"status": "error", "message": "STT phải là số nguyên!"}), 400

        # 2. Kiểm tra rating
        try:
            rating_val = float(data.get('rating', 5.0))
            if rating_val < 1.0 or rating_val > 5.0:
                return jsonify({"status": "error", "message": "Rating phải thuộc khoảng [1.0, 5.0]!"}), 400
            rating = round(rating_val, 1)
        except (ValueError, TypeError):
            return jsonify({"status": "error", "message": "Rating không hợp lệ!"}), 400

        user_name = str(data.get('user_name', '')).strip()[:100] or 'Khách du lịch'
        review_text = str(data.get('review_text', '')).strip()[:1000]
        session_id = str(data.get('session_id') or f"sess_{uuid.uuid4().hex[:8]}").strip()
        event_id = str(uuid.uuid4())

        # 3. Kiểm tra tính sẵn sàng của hạ tầng Speed Layer (Spark Structured Streaming)
        if not materializer.is_streaming_active(max_age_seconds=25):
            return jsonify({
                "status": "error",
                "error_code": "STREAMING_OFFLINE",
                "message": "Spark Structured Streaming đang ngoại tuyến hoặc mất nhịp tim (>25s)! Vui lòng kiểm tra Speed Layer."
            }), 503

        # 4. Tạo sự kiện chuẩn theo schema quy định
        spot_info = materializer.get_spot_lambda_view(stt) or {}
        input_event = {
            "event_id": event_id,
            "event_type": "REVIEW",
            "action": "NEW_COMMUNITY_REVIEW",
            "stt": stt,
            "name": spot_info.get("name", f"Địa điểm #{stt}"),
            "location": spot_info.get("location", ""),
            "latitude": spot_info.get("latitude"),
            "longitude": spot_info.get("longitude"),
            "image": spot_info.get("image", ""),
            "description": spot_info.get("description", ""),
            "keywords": spot_info.get("keywords", []),
            "session_id": session_id,
            "user_name": user_name,
            "rating": rating,
            "review_rating": rating,
            "review_text": review_text,
            "is_user_review": True,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "t_backend_ms": int(time.time() * 1000),
            "source": "web"
        }

        # 5. [Big Data Pipeline] Publish vào Kafka topic 'tourism-input' (acks='all')
        # Bảo toàn trọn vẹn luồng: A -> Kafka input -> Spark Streaming -> Kafka output -> Web SSE -> B
        try:
            publish(
                topic=TOPIC_INPUT,
                key=session_id,
                value=input_event,
                bootstrap_servers=DEFAULT_BOOTSTRAP_SERVERS
            )
        except Exception as e_pub:
            return jsonify({
                "status": "error",
                "error_code": "KAFKA_PUBLISH_FAILED",
                "message": f"Không thể gửi thông điệp vào Kafka Broker: {e_pub}"
            }), 503

        # Trả về HTTP 202 Accepted: Đang chờ Spark Structured Streaming xử lý
        return jsonify({
            "status": "processing",
            "event_id": event_id,
            "session_id": session_id,
            "message": f"Đã gửi thành công sự kiện #{event_id} vào Kafka 'tourism-input' (đang chờ Spark Streaming xử lý)...",
            "stt": stt
        }), 202

    except Exception as e:
        return jsonify({
            "status": "error",
            "message": f"Lỗi nội bộ máy chủ khi gửi đánh giá: {e}"
        }), 500


@app.route('/api/recommend', methods=['GET', 'POST'])
def api_recommend():
    """
    Gợi ý thông minh dựa trên kết quả TF-IDF Cosine & FP-Growth từ State Store RAM.
    """
    if not materializer.has_batch_commit():
        return jsonify({
            "status": "waiting_batch",
            "message": "Chạy spark_batch_train.py",
            "recommendations": [],
            "fp_growth": {"suggested_activities": []}
        }), 200

    if request.method == 'POST':
        req_data = request.json or {}
        kw_input = req_data.get('keywords', [])
    else:
        kw_str = request.args.get('keywords', '')
        kw_input = kw_str.split(',') if kw_str else []

    if isinstance(kw_input, str):
        kw_input = [k.strip() for k in kw_input.split(',') if k.strip()]

    query_keywords = [k.lower().strip() for k in kw_input if k.strip()]
    if not query_keywords:
        query_keywords = ["biển", "chụp ảnh"]

    q_text = " ".join(query_keywords).lower()
    spark_preset_map = {
        "bien": ["biển", "tắm biển", "hải sản", "nghĩ dưỡng", "bình minh"],
        "tam_linh": ["chùa", "tâm linh", "di tích", "lịch sử", "văn hóa"],
        "trekking": ["núi", "thác", "trekking", "khám phá", "thiên nhiên"],
        "gia_dinh": ["gia đình", "trẻ em", "an toàn", "ẩm thực", "check-in"],
        "cap_doi": ["cặp đôi", "lãng mạn", "hoàng hôn", "bình minh", "hẹn hò", "ngắm cảnh"]
    }

    matched_preset = None
    for p_key, p_words in spark_preset_map.items():
        if any(w in q_text for w in p_words):
            matched_preset = p_key
            break

    query_recs_model = materializer.get_batch_model("query_recs") or materializer.get_batch_model("query_recommendations")
    if query_recs_model and isinstance(query_recs_model, dict):
        raw_recs = query_recs_model.get("query_recommendations", query_recs_model)
        if matched_preset and matched_preset in raw_recs:
            res = copy.deepcopy(raw_recs[matched_preset])
            recs = res.get("recommendations", [])
            for r in recs:
                stt = r.get("stt")
                if stt is not None:
                    spot_lambda = materializer.get_spot_lambda_view(int(stt))
                    if spot_lambda and "rating" in spot_lambda:
                        r["rating"] = round(float(spot_lambda["rating"]), 1)

            return jsonify({
                "status": "success",
                "query_keywords": query_keywords,
                "matched_preset": matched_preset,
                "recommendations": recs,
                "fp_growth": res.get("fp_growth", {"suggested_activities": ["tham quan", "khám phá"]}),
                "source": "Apache Spark MLlib Tokenizer + HashingTF + IDFModel + FP-Growth via Kafka Materialized View"
            })

    return jsonify({
        "status": "not_computed",
        "query_keywords": query_keywords,
        "message": f"Truy vấn tùy biến ('{q_text}') chưa có trong mô hình vector hóa. Hãy chọn phong cách du lịch được đào tạo sẵn.",
        "supported_presets": list(spark_preset_map.keys()),
        "recommendations": [],
        "fp_growth": {"suggested_activities": []}
    }), 200


@app.route('/api/itinerary_multiday', methods=['GET', 'POST'])
def api_multiday_itinerary():
    """
    Lấy lịch trình đa ngày tối ưu (1..5 ngày) theo thuật toán Haversine từ RAM.
    """
    if not materializer.has_batch_commit():
        return jsonify({
            "status": "waiting_batch",
            "message": "Chạy spark_batch_train.py",
            "itinerary": []
        }), 200

    region = request.args.get('province') or request.args.get('region') or 'Khánh Hòa'
    try:
        days = int(request.args.get('days', 3))
    except (ValueError, TypeError):
        days = 3

    itin_data = materializer.get_batch_itinerary(region=region, days=days)
    if itin_data:
        itin_data = copy.deepcopy(itin_data)
        # Đồng bộ số sao thực tế từ Lambda View thời gian thực của từng địa điểm
        for day_plan in itin_data:
            for stop in day_plan.get("stops", []):
                stt = stop.get("stt")
                if stt is not None:
                    spot_lambda = materializer.get_spot_lambda_view(int(stt))
                    if spot_lambda and "rating" in spot_lambda:
                        stop["rating"] = round(float(spot_lambda["rating"]), 1)
            for sp in day_plan.get("spots", []):
                stt = sp.get("stt")
                if stt is not None:
                    spot_lambda = materializer.get_spot_lambda_view(int(stt))
                    if spot_lambda and "rating" in spot_lambda:
                        sp["rating"] = round(float(spot_lambda["rating"]), 1)

        return jsonify({
            "status": "success",
            "region": region,
            "days": days,
            "source": "Kafka Materialized Lambda View (RAM)",
            "algorithm": "Heuristic Tham Lam Láng Giềng Gần Nhất (Greedy Nearest Neighbor)",
            "itinerary": itin_data
        })

    return jsonify({
        "status": "waiting_spark",
        "region": region,
        "days": days,
        "message": f"Chưa tìm thấy lịch trình cho '{region}' ({days} ngày) trong State Store.",
        "itinerary": []
    }), 404


# =============================================================================
# ĐIỀU PHỐI BATCH RETRAIN & GIÁM SÁT TIẾN TRÌNH
# =============================================================================

@app.route('/api/sync_hdfs_retrain', methods=['POST', 'GET'])
def api_sync_hdfs_retrain():
    """
    Kích hoạt Job Batch Spark MLlib tái huấn luyện trên HDFS:
    - Khóa đảm bảo chỉ 1 job chạy tại 1 thời điểm.
    - Chạy spark_batch_train.py trong luồng nền với --job-id.
    - Trả về HTTP 202 Accepted.
    """
    global _active_sync_job_id, _active_sync_process

    with _batch_sync_lock:
        if _active_sync_job_id is not None:
            return jsonify({
                "status": "already_running",
                "job_id": _active_sync_job_id,
                "message": f"Job batch #{_active_sync_job_id} đang chạy. Vui lòng đợi hoàn thành!"
            }), 409

        req_data = request.json if (request.is_json and request.json) else {}
        job_id = req_data.get('job_id') or request.args.get('job_id') or f"batch_{int(time.time() * 1000)}"
        _active_sync_job_id = job_id

    def _run_batch_worker(assigned_job_id: str):
        global _active_sync_job_id, _active_sync_process
        print(f"[*] [Batch Sync Thread] Khởi động job batch #{assigned_job_id}...")
        train_script = os.path.join(BASE_DIR, "spark", "spark_batch_train.py")
        cmd = [sys.executable, train_script, "--job-id", assigned_job_id]

        try:
            _active_sync_process = subprocess.Popen(cmd)
            _active_sync_process.wait()
            ret_code = _active_sync_process.returncode
            print(f"[+] [Batch Sync Thread] Job #{assigned_job_id} hoàn tất với mã thoát: {ret_code}")
        except Exception as e_proc:
            print(f"[-] [Batch Sync Thread] Lỗi thực thi subprocess batch: {e_proc}")
        finally:
            with _batch_sync_lock:
                _active_sync_job_id = None
                _active_sync_process = None

    sync_thread = threading.Thread(target=_run_batch_worker, args=(job_id,), daemon=True)
    sync_thread.start()

    return jsonify({
        "status": "accepted",
        "job_id": job_id,
        "success": True,
        "message": f"Đã khởi động job batch huấn luyện #{job_id}. Đang đồng bộ HDFS và huấn luyện PySpark MLlib..."
    }), 202


@app.route('/api/sync_status')
def get_sync_status():
    """
    Truy vấn tiến độ của batch job theo job_id từ Kafka topic 'tourism-control'.
    """
    job_id = request.args.get('job_id')
    if not job_id:
        return jsonify({"status": "error", "message": "Thiếu tham số job_id!"}), 400

    ctrl_state = materializer.get_batch_job_status(job_id)
    if ctrl_state:
        return jsonify(ctrl_state)

    # Nếu đang chạy nhưng chưa publish control record
    if _active_sync_job_id == job_id:
        return jsonify({
            "job_id": job_id,
            "status": "RUNNING",
            "step": "Initializing Spark Batch job..."
        })

    return jsonify({
        "job_id": job_id,
        "status": "PENDING",
        "message": "Chưa có thông tin tiến trình."
    })


@app.route('/api/health/lambda')
def get_lambda_health():
    """
    API kiểm tra sức khỏe Lambda Architecture:
    - heartbeat age (tuổi của heartbeat streaming tính bằng giây)
    - active job_id
    - trạng thái materializer
    - số spot/review trong view
    - danh sách topic
    """
    hb_info = materializer.get_streaming_heartbeat()
    heartbeat_age = None
    if hb_info and hb_info.get("ts_ms"):
        try:
            heartbeat_age = round((time.time() * 1000 - float(hb_info["ts_ms"])) / 1000.0, 1)
        except Exception:
            pass

    batch_commit_info = materializer.get_active_batch_info()
    active_job_id = batch_commit_info.get("job_id") if batch_commit_info else None

    # Đo lag của topic tourism-output nếu Kafka đang online
    lag_output = 0
    try:
        s_k = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s_k.settimeout(0.3)
        kafka_live = (s_k.connect_ex(('localhost', 9092)) == 0)
        s_k.close()
        if kafka_live:
            from kafka import KafkaConsumer, TopicPartition
            cons = KafkaConsumer(bootstrap_servers=DEFAULT_BOOTSTRAP_SERVERS, api_version=(2, 8, 1), request_timeout_ms=1000)
            parts = cons.partitions_for_topic(TOPIC_OUTPUT)
            if parts:
                tps = [TopicPartition(TOPIC_OUTPUT, p) for p in parts]
                ends = cons.end_offsets(tps)
                total_end = sum(ends.values())
                lag_output = total_end - len(seen_sse_set) if total_end >= len(seen_sse_set) else 0
            cons.close()
    except Exception:
        pass

    return jsonify({
        "status": "HEALTHY",
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "materializer": {
            "status": "READY" if materializer.has_batch_commit() else "WAITING_BATCH",
            "active_batch_job_id": active_job_id,
            "committed_spots_count": len(materializer.get_lambda_spot_views()),
            "reviews_in_view": materializer.get_reviews_count(),
            "has_batch_commit": materializer.has_batch_commit()
        },
        "streaming": {
            "is_active": materializer.is_streaming_active(max_age_seconds=25),
            "heartbeat_age_seconds": heartbeat_age,
            "session_id": hb_info.get("session_id") if hb_info else None,
            "run_id": hb_info.get("run_id") if hb_info else None,
            "last_batch_id": hb_info.get("last_batch_id") if hb_info else None
        },
        "kafka": {
            "bootstrap_servers": DEFAULT_BOOTSTRAP_SERVERS,
            "lag_tourism_output": max(0, lag_output),
            "topics": [
                TOPIC_INPUT,
                TOPIC_OUTPUT,
                TOPIC_REVIEWS,
                TOPIC_STATE_SPOTS,
                TOPIC_STATE_BATCH,
                TOPIC_CONTROL,
                TOPIC_DEAD_LETTER
            ]
        }
    })


@app.route('/api/system_status')
def get_system_status():
    """Kiểm tra cổng kết nối hạ tầng phục vụ hiển thị trên thanh trạng thái."""
    # 1. Kafka Port Probe
    s_k = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s_k.settimeout(0.4)
    kafka_online = (s_k.connect_ex(('localhost', 9092)) == 0)
    s_k.close()

    # 2. HDFS Port Probe
    s_h = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s_h.settimeout(0.4)
    hdfs_online = (s_h.connect_ex(('localhost', 9000)) == 0)
    s_h.close()

    streaming_active = materializer.is_streaming_active(max_age_seconds=25)
    has_batch = materializer.has_batch_commit()

    return jsonify({
        "kafka": {
            "status": "ONLINE" if kafka_online else "OFFLINE",
            "port": 9092,
            "connected": kafka_online
        },
        "hdfs": {
            "status": "ONLINE" if hdfs_online else "OFFLINE",
            "port": 9000,
            "connected": hdfs_online
        },
        "spark": {
            "status": "ACTIVE" if streaming_active else "OFFLINE",
            "has_batch_commit": has_batch,
            "streaming_active": streaming_active,
            "spots_in_catalog": len(materializer.get_lambda_spot_views())
        },
        "web": {
            "status": "ONLINE",
            "active_subscribers": len(subscribers)
        }
    })


if __name__ == '__main__':
    print("[*] Khởi chạy Tourism Real-Time Web Dashboard trên cổng 5000...")
    app.run(host='0.0.0.0', port=5000, debug=False, threaded=True)
