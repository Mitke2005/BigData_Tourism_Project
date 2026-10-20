"""
KAFKA MATERIALIZED VIEW SERVICE (IN-MEMORY SERVING LAYER)
=============================================================================
Kiến trúc Kafka-First:
1. Dùng MỘT KafkaConsumer gán thủ công partition (manual assign, không commit offset).
2. Đọc từ đầu các topic:
   - tourism-state-spots (Batch view từng địa điểm)
   - tourism-state-batch (Mô hình, lịch trình, batch_commit)
   - tourism-reviews (Log review thời gian thực chuẩn)
   - tourism-control (Heartbeat streaming và trạng thái batch job)
3. Duy trì In-Memory Materialized View trong RAM với RLock thread-safe.
4. Cơ chế Batch Generation Isolation:
   Chỉ áp dụng và hiển thị bản ghi của job_id khi nhận được thông điệp 'batch_commit'
   với status='COMMITTED'. Bản ghi của job chưa commit được giữ ở pending.
5. Cung cấp API phục vụ Web Dashboard không phụ thuộc cơ sở dữ liệu.
=============================================================================
"""

import os
import sys
import json
import time
import copy
import threading
from typing import Dict, List, Any, Optional, Set

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from kafka import KafkaConsumer, TopicPartition

from messaging.state_store import (
    TOPIC_STATE_SPOTS,
    TOPIC_STATE_BATCH,
    TOPIC_REVIEWS,
    TOPIC_CONTROL,
    DEFAULT_BOOTSTRAP_SERVERS
)
from serving.lambda_view import compute_spot_lambda_view, sanitize_text

if sys.platform.startswith('win'):
    try:
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
        sys.stderr.reconfigure(encoding='utf-8', errors='replace')
    except Exception:
        pass

class KafkaMaterializer:
    def __init__(self, bootstrap_servers: Optional[str] = None):
        self.bootstrap_servers = bootstrap_servers or DEFAULT_BOOTSTRAP_SERVERS
        self._lock = threading.RLock()
        self._running = False
        self._worker_thread: Optional[threading.Thread] = None
        self._ready_event = threading.Event()
        
        # 1. Trạng thái cam kết (Active Materialized View)
        self._committed_batch_commit: Optional[Dict[str, Any]] = None
        self._synced_event_ids: Set[str] = set()
        self._committed_spots: Dict[int, Dict[str, Any]] = {}
        self._committed_models: Dict[str, Any] = {}
        self._committed_itineraries: Dict[str, Any] = {}
        
        # 2. Vùng đệm chờ cam kết theo job_id (Pending Batch Data)
        self._pending_spots: Dict[str, Dict[int, Dict[str, Any]]] = {}
        self._pending_batch: Dict[str, Dict[str, Any]] = {}
        self._pending_commits: Dict[str, Dict[str, Any]] = {}
        
        # 3. Log đánh giá thời gian thực (Keyed by event_id)
        self._reviews_by_event_id: Dict[str, Dict[str, Any]] = {}
        self._reviews_by_stt: Dict[int, Dict[str, Dict[str, Any]]] = {}
        
        # 4. Trạng thái điều khiển hệ thống (Control Status)
        self._control_state: Dict[str, Dict[str, Any]] = {}
        
        # Metadata tiến trình đọc
        self._initial_end_offsets: Dict[TopicPartition, int] = {}
        self._target_partitions: List[TopicPartition] = []

    def start(self):
        """Khởi động luồng nền duy nhất để đọc Kafka topics và xây dựng view trong RAM."""
        with self._lock:
            if self._running:
                return
            self._running = True
            self._worker_thread = threading.Thread(target=self._run_consumer_loop, daemon=True, name="KafkaMaterializerThread")
            self._worker_thread.start()

    def stop(self):
        """Dừng tiến trình tiêu thụ dữ liệu."""
        with self._lock:
            self._running = False
            self._ready_event.clear()
        if self._worker_thread and self._worker_thread.is_alive():
            self._worker_thread.join(timeout=3.0)

    def wait_until_ready(self, timeout: float = 30.0) -> bool:
        """Chờ cho tới khi consumer đọc bắt kịp end_offsets ban đầu lúc khởi động."""
        return self._ready_event.wait(timeout=timeout)

    def is_ready(self) -> bool:
        """Kiểm tra cờ sẵn sàng của Materialized View."""
        return self._ready_event.is_set()

    def _run_consumer_loop(self):
        backoff = 1.0
        max_backoff = 15.0

        while self._running:
            # Xóa trạng thái ready của phiên trước khi bắt đầu phiên kết nối / thử lại mới
            self._ready_event.clear()
            consumer = None
            try:
                consumer = KafkaConsumer(
                    bootstrap_servers=self.bootstrap_servers,
                    enable_auto_commit=False,
                    auto_offset_reset='earliest',
                    key_deserializer=lambda k: k.decode('utf-8') if k is not None else None,
                    value_deserializer=lambda v: json.loads(v.decode('utf-8')) if v is not None else None,
                    api_version=(2, 8, 1),
                    consumer_timeout_ms=1000
                )

                # Lấy danh sách partition của 4 topics cần theo dõi
                topics_to_watch = [TOPIC_STATE_SPOTS, TOPIC_STATE_BATCH, TOPIC_REVIEWS, TOPIC_CONTROL]
                assigned_partitions = []

                # Đảm bảo kết nối và lấy metadata
                for attempt in range(10):
                    if not self._running:
                        break
                    assigned_partitions = []
                    for t in topics_to_watch:
                        parts = consumer.partitions_for_topic(t)
                        if parts:
                            for p in parts:
                                assigned_partitions.append(TopicPartition(t, p))
                    if assigned_partitions:
                        break
                    time.sleep(1.0)

                if not assigned_partitions:
                    print(f"[-] KafkaMaterializer: Chưa tìm thấy partition nào cho các topics. Thử lại sau {round(backoff, 1)}s...", flush=True)
                    self._ready_event.clear()
                    time.sleep(backoff)
                    backoff = min(backoff * 1.5, max_backoff)
                    continue

                self._target_partitions = assigned_partitions
                consumer.assign(assigned_partitions)
                consumer.seek_to_beginning()

                # Chụp end_offsets tại thời điểm bắt đầu phiên kết nối này
                self._initial_end_offsets = consumer.end_offsets(assigned_partitions)

                # Chỉ bật ready nếu thực sự tất cả partition ban đầu đều trống
                caught_up = False
                if all(self._initial_end_offsets.get(tp, 0) == 0 for tp in assigned_partitions):
                    caught_up = True
                    self._ready_event.set()
                else:
                    self._ready_event.clear()

                backoff = 1.0  # Reset backoff khi kết nối thành công

                while self._running:
                    records = consumer.poll(timeout_ms=500)

                    if records:
                        with self._lock:
                            for tp, messages in records.items():
                                for msg in messages:
                                    self._process_single_message(msg.topic, msg.key, msg.value)

                    # Kiểm tra xem đã bắt kịp end_offsets ban đầu chưa
                    if not caught_up:
                        all_reached = True
                        for tp in assigned_partitions:
                            cur_pos = consumer.position(tp)
                            target_pos = self._initial_end_offsets.get(tp, 0)
                            if cur_pos < target_pos:
                                all_reached = False
                                break
                        if all_reached:
                            caught_up = True
                            self._ready_event.set()
                            print("[+] KafkaMaterializer: Đã bắt kịp end_offsets của phiên kết nối mới! Materialized View sẵn sàng.", flush=True)

            except Exception as e:
                # Xóa cờ sẵn sàng ngay khi gặp ngoại lệ / mất kết nối
                self._ready_event.clear()
                print(f"[-] Lỗi trong vòng lặp KafkaMaterializer: {e}. Thử kết nối lại sau {round(backoff, 1)}s...", flush=True)
                time.sleep(backoff)
                backoff = min(backoff * 1.5, max_backoff)
            finally:
                self._ready_event.clear()
                if consumer:
                    try:
                        consumer.close()
                    except Exception:
                        pass

    def _process_single_message(self, topic: str, key: Optional[str], value: Optional[Dict[str, Any]]):
        """Xử lý từng bản ghi theo topic và cập nhật Materialized View trong RAM."""
        if value is None:
            # Thông điệp tombstone (xóa key)
            return

        # 1. BATCH SPOTS TOPIC (Key = str(stt))
        if topic == TOPIC_STATE_SPOTS:
            stt = int(value.get("stt", key if (key and key.isdigit()) else 0))
            if not stt:
                return
            job_id = str(value.get("job_id", "")).strip()
            if not job_id:
                # Nếu không có job_id (dữ liệu trực tiếp không qua isolation), nạp thẳng vào committed
                self._committed_spots[stt] = value
            else:
                # Lưu vào vùng đệm pending của job_id
                if job_id not in self._pending_spots:
                    self._pending_spots[job_id] = {}
                self._pending_spots[job_id][stt] = value
                # Kiểm tra lại xem bản ghi này có giúp hoàn tất batch đang chờ commit không
                self._try_promote_batch(job_id)

        # 2. BATCH STATE TOPIC (Key = model:* | itinerary:* | batch_commit)
        elif topic == TOPIC_STATE_BATCH:
            key_str = str(key or "").strip()
            job_id = str(value.get("job_id", "")).strip()
            
            if key_str == "batch_commit" or key_str.startswith("commit:"):
                status = str(value.get("status", "")).upper()
                if status == "COMMITTED":
                    c_job_id = job_id or str(value.get("job_id", "")).strip()
                    if c_job_id:
                        self._pending_commits[c_job_id] = value
                        self._try_promote_batch(c_job_id)
                    else:
                        self._apply_batch_commit(value)
            elif key_str.startswith("model:"):
                model_name = key_str.split(":", 1)[1]
                if not job_id:
                    self._committed_models[model_name] = value
                else:
                    if job_id not in self._pending_batch:
                        self._pending_batch[job_id] = {}
                    self._pending_batch[job_id][key_str] = value
                    self._try_promote_batch(job_id)
            elif key_str.lower().startswith("itinerary:"):
                # Chuẩn hóa key lưu trữ thống nhất chữ thường
                norm_key = key_str.lower()
                if not job_id:
                    self._committed_itineraries[norm_key] = value
                else:
                    if job_id not in self._pending_batch:
                        self._pending_batch[job_id] = {}
                    self._pending_batch[job_id][norm_key] = value
                    self._try_promote_batch(job_id)

        # 3. REVIEWS LOG TOPIC (Key = event_id)
        elif topic == TOPIC_REVIEWS:
            ev_id = str(value.get("event_id", key or "")).strip()
            if not ev_id:
                return
            stt = int(value.get("stt", 0))
            
            # Ghi đè theo event_id bản sao độc lập (idempotent, last-wins, chống mutate)
            stored_review = copy.deepcopy(value)
            stored_review["user_name"] = sanitize_text(stored_review.get("user_name", "Khách du lịch")) or "Khách du lịch"
            stored_review["review_text"] = sanitize_text(stored_review.get("review_text", ""))
            self._reviews_by_event_id[ev_id] = stored_review
            if stt > 0:
                if stt not in self._reviews_by_stt:
                    self._reviews_by_stt[stt] = {}
                self._reviews_by_stt[stt][ev_id] = stored_review

        # 4. CONTROL TOPIC (Key = streaming | batch_job:<id>)
        elif topic == TOPIC_CONTROL:
            key_str = str(key or "").strip()
            if key_str:
                self._control_state[key_str] = value

    def _try_promote_batch(self, job_id: str) -> bool:
        """
        Chỉ thăng cấp batch sang active khi và chỉ khi:
        1. Đã nhận batch_commit hợp lệ của đúng job_id
        2. Đã nhận đủ số lượng spots dự kiến (spots_count)
        3. Đã nhận đủ số lượng itineraries dự kiến (itinerary_count) nếu có
        """
        if not job_id or job_id not in self._pending_commits:
            return False
            
        commit_record = self._pending_commits[job_id]
        expected_spots = int(commit_record.get("spots_count", 0))
        expected_itineraries = int(commit_record.get("itinerary_count", 0))
        
        # 1. Kiểm tra số lượng spots hiện có của job_id
        current_spots = self._pending_spots.get(job_id, {})
        if expected_spots > 0 and len(current_spots) < expected_spots:
            # Chưa nhận đủ địa điểm của job_id này, tiếp tục giữ pending
            return False
            
        # 2. Kiểm tra số lượng itineraries hiện có của job_id
        pending_batch_items = self._pending_batch.get(job_id, {})
        current_itin_count = sum(1 for k in pending_batch_items.keys() if k.lower().startswith("itinerary:"))
        if expected_itineraries > 0 and current_itin_count < expected_itineraries:
            # Chưa nhận đủ lịch trình của job_id này, tiếp tục giữ pending
            return False
            
        # Đã nhận ĐỦ toàn bộ dữ liệu của job_id -> tiến hành thăng cấp nguyên tử
        self._apply_batch_commit(commit_record)
        del self._pending_commits[job_id]
        return True

    def _apply_batch_commit(self, commit_record: Dict[str, Any]):
        """Áp dụng nguyên tử (Atomic promotion) các bản ghi của job_id đã commit."""
        job_id = str(commit_record.get("job_id", "")).strip()
        self._committed_batch_commit = commit_record
        
        # Cập nhật danh sách synced_event_ids
        raw_synced = commit_record.get("synced_event_ids", [])
        self._synced_event_ids = set(str(eid).strip() for eid in raw_synced)
        
        # Thăng cấp pending spots của job_id
        if job_id and job_id in self._pending_spots:
            self._committed_spots = self._pending_spots[job_id]
            # Xóa pending của job này để giải phóng RAM
            del self._pending_spots[job_id]
            
        # Thăng cấp pending batch items (models, itineraries) của job_id
        if job_id and job_id in self._pending_batch:
            pending_items = self._pending_batch[job_id]
            for k, v in pending_items.items():
                if k.startswith("model:"):
                    m_name = k.split(":", 1)[1]
                    self._committed_models[m_name] = v
                elif k.lower().startswith("itinerary:"):
                    self._committed_itineraries[k.lower()] = v
            del self._pending_batch[job_id]
            
        print(f"[+] KafkaMaterializer: Đã nhận đủ dữ liệu và áp dụng batch_commit thành công (Job: {job_id}, Spots: {len(self._committed_spots)}, Synced Reviews: {len(self._synced_event_ids)})")

    # =========================================================================
    # CÁC PHƯƠNG THỨC TRUY VẤN VIEW CHO WEB & API (READ-ONLY, THREAD-SAFE)
    # =========================================================================

    def get_lambda_spot_views(self) -> List[Dict[str, Any]]:
        """Trả về toàn bộ danh mục địa điểm đã được hợp nhất Lambda View."""
        with self._lock:
            if not self._committed_spots:
                return []
                
            result = []
            for stt, spot in sorted(self._committed_spots.items(), key=lambda x: x[0]):
                rt_reviews_map = self._reviews_by_stt.get(stt, {})
                merged = compute_spot_lambda_view(
                    spot_state=spot,
                    reviews_list=list(rt_reviews_map.values()),
                    synced_event_ids=self._synced_event_ids
                )
                result.append(merged)
            return result

    def get_spot_lambda_view(self, stt: int) -> Optional[Dict[str, Any]]:
        """Trả về chi tiết 1 địa điểm theo STT từ Materialized Lambda View."""
        with self._lock:
            spot = self._committed_spots.get(int(stt))
            if not spot:
                return None
            rt_reviews_map = self._reviews_by_stt.get(int(stt), {})
            return compute_spot_lambda_view(
                spot_state=spot,
                reviews_list=list(rt_reviews_map.values()),
                synced_event_ids=self._synced_event_ids
            )

    def get_spot_reviews(self, stt: int, limit: int = 50) -> Dict[str, Any]:
        """Trả về toàn bộ nhận xét của 1 địa điểm (phục vụ giao diện bình luận Facebook)."""
        with self._lock:
            spot_view = self.get_spot_lambda_view(stt)
            rt_reviews_map = self._reviews_by_stt.get(int(stt), {})
            
            all_revs = sorted(
                rt_reviews_map.values(),
                key=lambda x: str(x.get("timestamp", "")),
                reverse=True
            )
            
            return {
                "status": "success",
                "spot": spot_view,
                "reviews": all_revs[:limit],
                "total_reviews": len(all_revs)
            }

    def get_batch_itinerary(self, region: str, days: int) -> Optional[Dict[str, Any]]:
        """Lấy lịch trình mẫu theo vùng miền và số ngày (chuẩn hóa không phân biệt chữ hoa/thường)."""
        reg_clean = region.strip().lower()
        days_int = int(days)
        cache_key = f"itinerary:{reg_clean}:{days_int}"
        with self._lock:
            # 1. Tìm trực tiếp theo key chuẩn hóa lowercase
            item = self._committed_itineraries.get(cache_key)
            if item:
                return item.get("itinerary", item)
            # 2. Tìm kiếm dự phòng không phân biệt hoa/thường
            for k, val in self._committed_itineraries.items():
                k_lower = k.lower()
                if f":{days_int}" in k_lower and reg_clean in k_lower:
                    return val.get("itinerary", val)
            return None

    def has_review_event(self, event_id: str) -> bool:
        """Kiểm tra xem review đã được nạp vào RAM Materializer chưa."""
        if not event_id:
            return False
        with self._lock:
            return str(event_id).strip() in self._reviews_by_event_id

    def ingest_live_review(self, review_dict: Dict[str, Any]):
        """
        Nạp tức thì một review vào RAM (idempotent) để bảo đảm không bao giờ phát điểm cũ qua SSE.
        """
        if not isinstance(review_dict, dict):
            return
        ev_id = str(review_dict.get("event_id", "")).strip()
        if not ev_id:
            return
        stt = int(review_dict.get("stt", 0))
        # Lưu bản sao độc lập (deepcopy) để ngăn chặn hoàn toàn việc mutate rating từ bên ngoài (ví dụ SSE)
        stored_review = copy.deepcopy(review_dict)
        stored_review["user_name"] = sanitize_text(stored_review.get("user_name", "Khách du lịch")) or "Khách du lịch"
        stored_review["review_text"] = sanitize_text(stored_review.get("review_text", ""))
        with self._lock:
            self._reviews_by_event_id[ev_id] = stored_review
            if stt > 0:
                if stt not in self._reviews_by_stt:
                    self._reviews_by_stt[stt] = {}
                self._reviews_by_stt[stt][ev_id] = stored_review

    def get_batch_model(self, key: str) -> Optional[Dict[str, Any]]:
        """Lấy thông tin mô hình học máy (kmeans, rules, query_recs)."""
        with self._lock:
            return self._committed_models.get(key.strip())

    def is_streaming_active(self, max_age_seconds: int = 25, expected_session_id: Optional[str] = None) -> bool:
        """Kiểm tra nhịp tim hoạt động của Spark Structured Streaming từ topic tourism-control."""
        with self._lock:
            stream_ctrl = self._control_state.get("streaming")
            if not stream_ctrl:
                return False
            status = str(stream_ctrl.get("status", "")).upper()
            if status != "ACTIVE":
                return False
                
            ts_ms = stream_ctrl.get("ts_ms")
            if not ts_ms:
                return False
                
            age_sec = (time.time() * 1000 - float(ts_ms)) / 1000.0
            if age_sec > max_age_seconds:
                return False
                
            if expected_session_id and stream_ctrl.get("session_id") != expected_session_id:
                return False
                
            return True

    def get_active_batch_info(self) -> Optional[Dict[str, Any]]:
        """Lấy thông tin tóm tắt về batch_commit đang kích hoạt."""
        with self._lock:
            return self._committed_batch_commit

    def get_batch_commit(self) -> Optional[Dict[str, Any]]:
        """Bí danh của get_active_batch_info()."""
        return self.get_active_batch_info()

    def has_batch_commit(self) -> bool:
        """Kiểm tra xem đã nhận và thăng cấp bản ghi batch_commit nào chưa."""
        return self.get_active_batch_info() is not None

    def get_batch_job_status(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Lấy trạng thái thực thi của batch job từ topic tourism-control."""
        with self._lock:
            return self._control_state.get(f"batch_job:{job_id}")

    def get_streaming_heartbeat(self) -> Optional[Dict[str, Any]]:
        """Lấy thông tin nhịp tim mới nhất của Spark Streaming từ topic tourism-control."""
        with self._lock:
            return self._control_state.get("streaming")

    def get_reviews_count(self) -> int:
        """Lấy tổng số bài đánh giá duy nhất đã nhận trong RAM Materialized View."""
        with self._lock:
            return len(self._reviews_by_event_id)

# Singleton instance phục vụ toàn ứng dụng Web
_materializer_singleton = None
_singleton_lock = threading.Lock()

def get_materializer(bootstrap_servers: Optional[str] = None) -> KafkaMaterializer:
    global _materializer_singleton
    with _singleton_lock:
        if _materializer_singleton is None:
            _materializer_singleton = KafkaMaterializer(bootstrap_servers)
            _materializer_singleton.start()
        return _materializer_singleton
