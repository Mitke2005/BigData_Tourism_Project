"""
TIẾN TRÌNH LƯU TRỮ LỊCH SỬ HDFS TÁCH BIỆT (HDFS STREAM ARCHIVER)
=============================================================================
Kiến trúc Kafka-First:
1. Tách biệt hoàn toàn việc ghi HDFS ra khỏi đường phản hồi thời gian thực:
   - Đường phản hồi trực tiếp: A -> Kafka input -> Spark -> Kafka output -> Web SSE -> B.
   - Đường lưu trữ lịch sử: Kafka output / reviews -> hdfs_stream_archiver -> HDFS Parquet Lake.
2. Quản lý trạng thái & độ bền (Persistent Idempotent Storage):
   - Định danh ghi bền vững theo topic/partition/offset:
     hdfs://.../tourism_data/streaming_zone/{subzone}/run_id=p{partition}/batch_id={min_offset}_{max_offset}/
   - Lưu trữ manifest trạng thái checkpoint bền vững trên HDFS (_archiver_manifest.json).
   - Khử trùng lặp idempotent theo (topic, partition, offset) ngay cả khi poll chia batch khác trước hoặc sau restart.
   - Xử lý và commit độc lập giữa events và reviews: sự kiện nào ghi thành công thì cập nhật manifest và commit offset sự kiện đó. Nếu lỗi một bên, không ghi đè hay xóa bộ đệm của bên lỗi.
3. Ghi Parquet nén Snappy theo cấu trúc chuẩn tương thích Spark Compaction.
=============================================================================
"""

import os
import sys
import json
import time
import uuid
import socket
import tempfile
import subprocess
from datetime import datetime
from typing import Dict, List, Any, Optional

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

    for selector_cls in [getattr(selectors, 'SelectSelector', None), getattr(selectors, 'DefaultSelector', None)]:
        if selector_cls and hasattr(selector_cls, 'unregister'):
            _orig_unregister = selector_cls.unregister
            def _make_safe_unregister(orig_fn):
                def _safe_unregister(self, fileobj):
                    try:
                        return orig_fn(self, fileobj)
                    except (KeyError, ValueError):
                        return None
                return _safe_unregister
            selector_cls.unregister = _make_safe_unregister(_orig_unregister)
except Exception:
    pass

if sys.platform.startswith('win'):
    try:
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
        sys.stderr.reconfigure(encoding='utf-8', errors='replace')
    except Exception:
        pass

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

import pandas as pd

try:
    from kafka import KafkaConsumer, TopicPartition, OffsetAndMetadata
except ImportError:
    from kafka_python_ng import KafkaConsumer, TopicPartition, OffsetAndMetadata  # type: ignore

from messaging.state_store import (
    TOPIC_OUTPUT,
    TOPIC_REVIEWS,
    DEFAULT_BOOTSTRAP_SERVERS
)

HADOOP_HOME = os.environ.get("HADOOP_HOME", "C:/hadoop" if sys.platform.startswith('win') else "/usr/local/hadoop")
HADOOP_HOME = os.path.abspath(HADOOP_HOME).replace('\\', '/')
HDFS_URI = os.environ.get("HDFS_URI", "hdfs://localhost:9000")

LOCAL_MANIFEST_PATH = os.path.join(BASE_DIR, "spark", "archiver_manifest.json")
HDFS_MANIFEST_DIR = f"{HDFS_URI}/tourism_data/streaming_zone"
HDFS_MANIFEST_PATH = f"{HDFS_MANIFEST_DIR}/_archiver_manifest.json"


def get_hdfs_cmd() -> str:
    """Lấy đường dẫn thực thi lệnh hdfs."""
    hdfs_bin = "hdfs.cmd" if sys.platform.startswith('win') else "hdfs"
    candidate = os.path.join(HADOOP_HOME, "bin", hdfs_bin)
    return candidate if os.path.exists(candidate) else hdfs_bin


def is_hdfs_alive() -> bool:
    """Kiểm tra kết nối cổng NameNode HDFS 9000."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(0.5)
    try:
        s.connect(('localhost', 9000))
        s.close()
        return True
    except Exception:
        return False


def load_manifest() -> Dict[str, Any]:
    """
    Tải manifest trạng thái offset bền vững từ HDFS hoặc cache cục bộ.
    Hỗ trợ tương thích cả cấu trúc mới { topics, pending_batches } và cấu trúc phẳng { topic_name: { partition: offset } }.
    """
    manifest: Dict[str, Any] = {
        "topics": {
            TOPIC_OUTPUT: {},
            TOPIC_REVIEWS: {}
        },
        "pending_batches": {
            TOPIC_OUTPUT: {},
            TOPIC_REVIEWS: {}
        }
    }

    # 1. Thử tải từ HDFS nếu NameNode hoạt động
    if is_hdfs_alive():
        env = os.environ.copy()
        env["HADOOP_HOME"] = HADOOP_HOME
        hdfs_cmd = get_hdfs_cmd()
        try:
            res = subprocess.run(
                [hdfs_cmd, "dfs", "-cat", HDFS_MANIFEST_PATH],
                env=env,
                capture_output=True,
                text=True,
                timeout=10
            )
            if res.returncode == 0 and res.stdout.strip():
                data = json.loads(res.stdout)
                if "topics" in data:
                    for t, parts in data.get("topics", {}).items():
                        manifest["topics"][t] = {str(k): int(v) for k, v in parts.items()}
                else:
                    for t in [TOPIC_OUTPUT, TOPIC_REVIEWS]:
                        if t in data and isinstance(data[t], dict):
                            manifest["topics"][t] = {str(k): int(v) for k, v in data[t].items()}
                for t, p_batches in data.get("pending_batches", {}).items():
                    manifest["pending_batches"][t] = p_batches
                # Đồng bộ lại bản sao cục bộ
                with open(LOCAL_MANIFEST_PATH, "w", encoding="utf-8") as f:
                    json.dump({
                        "topics": manifest["topics"],
                        "pending_batches": manifest["pending_batches"],
                        "updated_at": datetime.now().isoformat()
                    }, f, indent=2)
                for t in [TOPIC_OUTPUT, TOPIC_REVIEWS]:
                    manifest[t] = manifest["topics"].setdefault(t, {})
                return manifest
        except Exception:
            pass

    # 2. Dự phòng đọc file manifest cục bộ nếu HDFS chưa có hoặc không đọc được
    if os.path.exists(LOCAL_MANIFEST_PATH):
        try:
            with open(LOCAL_MANIFEST_PATH, "r", encoding="utf-8") as f:
                data = json.load(f)
                if "topics" in data:
                    for t, parts in data.get("topics", {}).items():
                        manifest["topics"][t] = {str(k): int(v) for k, v in parts.items()}
                else:
                    for t in [TOPIC_OUTPUT, TOPIC_REVIEWS]:
                        if t in data and isinstance(data[t], dict):
                            manifest["topics"][t] = {str(k): int(v) for k, v in data[t].items()}
                for t, p_batches in data.get("pending_batches", {}).items():
                    manifest["pending_batches"][t] = p_batches
        except Exception:
            pass

    for t in [TOPIC_OUTPUT, TOPIC_REVIEWS]:
        manifest[t] = manifest["topics"].setdefault(t, {})
    return manifest


def save_manifest(manifest: Dict[str, Any]) -> bool:
    """
    Lưu manifest bền vững vào cả file cục bộ và HDFS.
    Kiểm tra chặt chẽ mã thoát subprocess và lỗi để báo chính xác cho bên gọi.
    """
    topics = manifest.get("topics")
    if topics is None:
        topics = {}
        for t in [TOPIC_OUTPUT, TOPIC_REVIEWS]:
            if t in manifest and isinstance(manifest[t], dict):
                topics[t] = manifest[t]

    payload = {
        "topics": topics,
        "pending_batches": manifest.get("pending_batches", {}),
        "updated_at": datetime.now().isoformat()
    }
    # 1. Lưu bản sao cục bộ
    local_saved = False
    try:
        with open(LOCAL_MANIFEST_PATH, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)
        local_saved = True
    except Exception as e:
        print(f"[!] [HDFS Archiver] Lỗi ghi manifest cục bộ: {e}", flush=True)

    # 2. Đẩy lên HDFS nếu khả dụng
    if is_hdfs_alive():
        env = os.environ.copy()
        env["HADOOP_HOME"] = HADOOP_HOME
        hdfs_cmd = get_hdfs_cmd()
        temp_fd, temp_file = tempfile.mkstemp(suffix="_manifest.json")
        os.close(temp_fd)
        try:
            with open(temp_file, "w", encoding="utf-8") as f:
                json.dump(payload, f, indent=2)
            temp_file_norm = temp_file.replace('\\', '/')

            res_mkdir = subprocess.run(
                [hdfs_cmd, "dfs", "-mkdir", "-p", HDFS_MANIFEST_DIR],
                env=env,
                capture_output=True,
                text=True,
                timeout=10
            )
            if res_mkdir.returncode != 0:
                print(f"[!] [HDFS Archiver] Lỗi mkdir manifest HDFS (code {res_mkdir.returncode}): {res_mkdir.stderr.strip()}", flush=True)
                return False

            res_put = subprocess.run(
                [hdfs_cmd, "dfs", "-put", "-f", temp_file_norm, HDFS_MANIFEST_PATH],
                env=env,
                capture_output=True,
                text=True,
                timeout=10
            )
            err_lower = (res_put.stderr or "").lower()
            is_failed = "exception" in err_lower or "failed" in err_lower or "error" in err_lower
            if res_put.returncode == 0 and not is_failed:
                return True
            else:
                print(f"[!] [HDFS Archiver] Lỗi ghi manifest lên HDFS (code {res_put.returncode}): {res_put.stderr.strip()}", flush=True)
                return False
        except Exception as e_h:
            print(f"[!] [HDFS Archiver] Ngoại lệ cập nhật manifest HDFS: {e_h}", flush=True)
            return False
        finally:
            if os.path.exists(temp_file):
                try:
                    os.remove(temp_file)
                except Exception:
                    pass
    return local_saved


def upload_parquet_to_hdfs(df: pd.DataFrame, hdfs_target_dir: str, max_retries: int = 3) -> bool:
    """
    Ghi DataFrame ra file Parquet tạm thời và đẩy lên HDFS với cơ chế retry và khử lỗi Windows CLI.
    """
    if df.empty:
        return True

    # Chuẩn hóa kiểu dữ liệu phức tạp để pyarrow không bị lỗi schema
    df_clean = df.copy()
    for str_col in ['event_id', 'session_id', 'user_name', 'review_text', 'name', 'location', 'cluster_name', 'color', 'timestamp', 'processed_at']:
        if str_col in df_clean.columns:
            df_clean[str_col] = df_clean[str_col].astype(str)
    if 'keywords' in df_clean.columns:
        df_clean['keywords'] = df_clean['keywords'].apply(
            lambda k: list(k) if isinstance(k, (list, tuple)) else ([str(k)] if pd.notnull(k) and str(k) != '' else [])
        )

    env = os.environ.copy()
    env["HADOOP_HOME"] = HADOOP_HOME
    hdfs_cmd = get_hdfs_cmd()

    temp_fd, temp_file = tempfile.mkstemp(suffix=f"_archiver_{uuid.uuid4().hex[:8]}.parquet")
    os.close(temp_fd)
    temp_file_normalized = temp_file.replace('\\', '/')

    try:
        df_clean.to_parquet(temp_file, engine='pyarrow', compression='snappy', index=False)

        if not is_hdfs_alive():
            print(f"[!] [HDFS Archiver] NameNode HDFS (localhost:9000) chưa sẵn sàng. Sẽ thử lại sau.", flush=True)
            return False

        for attempt in range(1, max_retries + 1):
            try:
                # 1. Tạo thư mục HDFS nếu chưa có
                subprocess.run(
                    [hdfs_cmd, "dfs", "-mkdir", "-p", hdfs_target_dir],
                    env=env,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    timeout=15
                )

                # 2. Đẩy file Parquet lên HDFS (chế độ overwrite)
                res = subprocess.run(
                    [hdfs_cmd, "dfs", "-put", "-f", temp_file_normalized, f"{hdfs_target_dir}/data.parquet"],
                    env=env,
                    capture_output=True,
                    text=True,
                    timeout=20
                )

                err_lower = (res.stderr or "").lower()
                is_failed = "exception" in err_lower or "failed" in err_lower or "error" in err_lower
                if res.returncode == 0 and not is_failed:
                    return True
                else:
                    if attempt == max_retries:
                        print(f"[!] [HDFS Archiver] Lỗi ghi HDFS: {res.stderr.strip()}", flush=True)
                    time.sleep(1.0 * attempt)
            except Exception as e_upload:
                if attempt == max_retries:
                    print(f"[!] [HDFS Archiver Retry Failed] Thử {attempt}/{max_retries} thất bại: {e_upload}", flush=True)
                time.sleep(1.0 * attempt)
        return False
    except Exception as e_parquet:
        print(f"[!] [HDFS Archiver] Lỗi chuyển đổi Parquet: {e_parquet}", flush=True)
        return False
    finally:
        if os.path.exists(temp_file):
            try:
                os.remove(temp_file)
            except Exception:
                pass


def safe_json_deserializer(m_bytes: bytes) -> Any:
    """Giải mã JSON an toàn, tránh poison-pill crash consumer."""
    try:
        return json.loads(m_bytes.decode('utf-8'))
    except Exception:
        return None


def run_hdfs_archiver():
    """Vòng lặp chính của tiến trình lưu trữ HDFS."""
    print("=" * 80, flush=True)
    print("  KHỞI ĐỘNG TIẾN TRÌNH LƯU TRỮ LỊCH SỬ HDFS (HDFS STREAM ARCHIVER)", flush=True)
    print("  Nhiệm vụ: Tiêu thụ Kafka -> Ghi Parquet HDFS bất đồng bộ (Zero Block Web/SSE)", flush=True)
    print(f"  Kafka: {DEFAULT_BOOTSTRAP_SERVERS} | HDFS: {HDFS_URI}", flush=True)
    print("=" * 80, flush=True)

    group_id = "hdfs_stream_archiver_v2"
    manifest = load_manifest()
    print(f"[+] [HDFS Archiver] Đã tải manifest trạng thái offset: {manifest}", flush=True)

    def create_consumer():
        c = None
        while c is None:
            try:
                c = KafkaConsumer(
                    TOPIC_OUTPUT,
                    TOPIC_REVIEWS,
                    bootstrap_servers=DEFAULT_BOOTSTRAP_SERVERS,
                    group_id=group_id,
                    auto_offset_reset='earliest',
                    enable_auto_commit=False,  # Manual commit checkpoint
                    value_deserializer=safe_json_deserializer,
                    api_version=(2, 8, 1)
                )
                print("[+] [HDFS Archiver] Đã kết nối thành công tới Kafka Topics!", flush=True)
                return c
            except Exception as e:
                print(f"[!] [HDFS Archiver] Đang đợi Kafka Broker ({e})...", flush=True)
                time.sleep(2.0)

    consumer = create_consumer()

    # 1. Khởi tạo active_batches từ manifest nếu có batch đang dang dở từ phiên trước
    active_batches: Dict[str, Dict[int, Dict[str, Any]]] = {
        TOPIC_OUTPUT: {},
        TOPIC_REVIEWS: {}
    }
    for t_name in [TOPIC_OUTPUT, TOPIC_REVIEWS]:
        p_batches = manifest.get("pending_batches", {}).get(t_name, {})
        for p_str, b_info in p_batches.items():
            try:
                p_id = int(p_str)
                min_off = int(b_info["min_offset"])
                max_off = int(b_info["max_offset"])
                raw_offsets = b_info.get("offsets")
                if raw_offsets:
                    exp_offsets = set(raw_offsets)
                else:
                    exp_offsets = set(range(min_off, max_off + 1))

                active_batches[t_name][p_id] = {
                    "subzone": b_info.get("subzone", "events" if t_name == TOPIC_OUTPUT else "reviews"),
                    "min_offset": min_off,
                    "max_offset": max_off,
                    "target_dir": b_info["target_dir"],
                    "expected_offsets": exp_offsets,
                    "records_by_offset": {},
                    "manifest_persisted": True,
                    "uploaded": bool(b_info.get("uploaded", False))
                }
                print(f"[*] [HDFS Archiver] Phục hồi pending batch cho '{t_name}':P{p_id} (offsets {min_off}->{max_off}, uploaded={b_info.get('uploaded', False)})", flush=True)
            except Exception as e_p:
                print(f"[!] [HDFS Archiver] Lỗi khôi phục pending batch {p_str}: {e_p}", flush=True)

    # 2. Bộ đệm nhận dữ liệu mới chờ thành lập batch tiếp theo: topic -> { partition: list_of_{offset, record} }
    incoming_buffers: Dict[str, Dict[int, List[Dict[str, Any]]]] = {
        TOPIC_OUTPUT: {},
        TOPIC_REVIEWS: {}
    }

    last_flush_time = time.time()
    FLUSH_INTERVAL_SEC = 5.0
    BATCH_SIZE_THRESHOLD = 50

    try:
        while True:
            try:
                records = consumer.poll(timeout_ms=1000)
            except Exception as e_poll:
                print(f"[!] [HDFS Archiver] Lỗi khi poll từ Kafka ({e_poll}). Đang thử kết nối lại...", flush=True)
                time.sleep(1.5)
                try:
                    consumer.close()
                except Exception:
                    pass
                consumer = create_consumer()
                continue
            now = time.time()

            for tp, msgs in records.items():
                topic_name = tp.topic
                part_id = tp.partition
                if topic_name not in incoming_buffers:
                    continue

                part_manifest_offset = manifest.get("topics", {}).get(topic_name, {}).get(str(part_id), -1)

                if part_id not in incoming_buffers[topic_name]:
                    incoming_buffers[topic_name][part_id] = []

                act_batch = active_batches[topic_name].get(part_id)

                for msg in msgs:
                    val = msg.value
                    if not isinstance(val, dict):
                        continue

                    # Idempotent: Bỏ qua nếu offset đã commit thành công trong manifest trước đó
                    if msg.offset <= part_manifest_offset:
                        continue

                    # Nếu partition đang có batch in-flight/chờ hoàn tất:
                    if act_batch is not None:
                        # Nếu rơi vào khoảng offset của batch in-flight:
                        if act_batch["min_offset"] <= msg.offset <= act_batch["max_offset"]:
                            # Nếu batch chưa upload thành công (ví dụ restart trước khi upload xong), nạp record vào batch
                            if not act_batch["uploaded"]:
                                act_batch["records_by_offset"][msg.offset] = val
                            continue

                        # Nếu msg.offset > act_batch["max_offset"]:
                        # Tuyệt đối KHÔNG ghép vào act_batch! Giữ riêng ở incoming_buffers để xử lý sau khi batch kia hoàn tất!
                        if not any(item["offset"] == msg.offset for item in incoming_buffers[topic_name][part_id]):
                            incoming_buffers[topic_name][part_id].append({
                                "offset": msg.offset,
                                "record": val
                            })
                    else:
                        # Partition chưa có batch in-flight: đưa vào bộ đệm nhận
                        if not any(item["offset"] == msg.offset for item in incoming_buffers[topic_name][part_id]):
                            incoming_buffers[topic_name][part_id].append({
                                "offset": msg.offset,
                                "record": val
                            })

            total_incoming = sum(len(plist) for top in incoming_buffers.values() for plist in top.values())
            has_active_batches = any(len(act_p) > 0 for act_p in active_batches.values())

            should_flush = (now - last_flush_time >= FLUSH_INTERVAL_SEC and (total_incoming > 0 or has_active_batches)) or \
                           (total_incoming >= BATCH_SIZE_THRESHOLD) or \
                           has_active_batches

            if should_flush:
                # Xử lý lưu trữ độc lập cho từng topic: nếu 1 topic lỗi, topic kia vẫn được commit an toàn
                for topic_name, subzone in [(TOPIC_OUTPUT, "events"), (TOPIC_REVIEWS, "reviews")]:
                    inc_dict = incoming_buffers[topic_name]
                    act_dict = active_batches[topic_name]

                    # 1. Đóng gói các partition chưa có active_batch thành batch mới cố định
                    # LƯU BỀN VỮNG MANIFEST PENDING TRƯỚC TIÊN (WRITE-AHEAD) TRƯỚC KHI GHI PARQUET
                    for part_id, items in list(inc_dict.items()):
                        if not items:
                            continue
                        if part_id in act_dict:
                            # Đang có batch dang dở cho partition này -> Phải retry hoàn tất trước khi ghép dữ liệu mới!
                            continue

                        items_sorted = sorted(items, key=lambda x: x["offset"])
                        min_off = items_sorted[0]["offset"]
                        max_off = items_sorted[-1]["offset"]
                        target_dir = f"{HDFS_URI}/tourism_data/streaming_zone/{subzone}/run_id=p{part_id}/batch_id={min_off}_{max_off}"
                        batch_offsets = [it["offset"] for it in items_sorted]

                        new_act = {
                            "subzone": subzone,
                            "min_offset": min_off,
                            "max_offset": max_off,
                            "target_dir": target_dir,
                            "expected_offsets": set(batch_offsets),
                            "records_by_offset": {it["offset"]: it["record"] for it in items_sorted},
                            "manifest_persisted": False,
                            "uploaded": False
                        }

                        # Lưu bền vững thông tin pending batch vào manifest TRƯỚC KHI ghi Parquet
                        if topic_name not in manifest.setdefault("pending_batches", {}):
                            manifest["pending_batches"][topic_name] = {}
                        manifest["pending_batches"][topic_name][str(part_id)] = {
                            "subzone": subzone,
                            "min_offset": min_off,
                            "max_offset": max_off,
                            "target_dir": target_dir,
                            "offsets": batch_offsets,
                            "uploaded": False
                        }

                        save_ok = save_manifest(manifest)
                        if save_ok:
                            new_act["manifest_persisted"] = True
                            act_dict[part_id] = new_act
                            inc_dict[part_id] = []
                            print(f"[*] [HDFS Archiver] Đã lưu trước manifest pending cho '{subzone}' (P{part_id}, batch {min_off}_{max_off}) vào {target_dir}", flush=True)
                        else:
                            # Lưu manifest pending thất bại -> Giữ nguyên trong incoming_buffers để thử lại, TUYỆT ĐỐI KHÔNG GHI PARQUET!
                            print(f"[!] [HDFS Archiver] Lưu manifest pending cho '{subzone}':P{part_id} thất bại. Chưa ghi Parquet, sẽ thử lại.", flush=True)

                    if not act_dict:
                        continue

                    # 2. Xử lý từng active batch theo đúng trình tự:
                    # Kiểm tra manifest pending -> Phục hồi đủ data -> Ghi Parquet -> Lưu manifest uploaded -> Commit Kafka -> Chốt manifest hoàn tất
                    for part_id, act in list(act_dict.items()):
                        # Bước 2.1: Đảm bảo manifest pending đã được lưu bền vững
                        if not act["manifest_persisted"]:
                            if topic_name not in manifest.setdefault("pending_batches", {}):
                                manifest["pending_batches"][topic_name] = {}
                            manifest["pending_batches"][topic_name][str(part_id)] = {
                                "subzone": subzone,
                                "min_offset": act["min_offset"],
                                "max_offset": act["max_offset"],
                                "target_dir": act["target_dir"],
                                "offsets": sorted(list(act["expected_offsets"])),
                                "uploaded": act["uploaded"]
                            }
                            if not save_manifest(manifest):
                                print(f"[!] [HDFS Archiver] Retry lưu manifest pending cho P{part_id} thất bại. Giữ nguyên batch, dừng ghi Parquet.", flush=True)
                                continue
                            act["manifest_persisted"] = True

                        # Bước 2.2: Ghi Parquet vào HDFS (nếu chưa upload)
                        if not act["uploaded"]:
                            # Kiểm tra đã phục hồi đủ dữ liệu cho batch chưa (trường hợp sau restart)
                            collected = set(act["records_by_offset"].keys())
                            if not act["expected_offsets"].issubset(collected):
                                missing = act["expected_offsets"] - collected
                                print(f"[*] [HDFS Archiver] Đang đợi phục hồi đủ {len(missing)} bản ghi cho batch {act['min_offset']}_{act['max_offset']} (P{part_id})...", flush=True)
                                continue

                            records_sorted = [act["records_by_offset"][o] for o in sorted(act["expected_offsets"])]
                            df = pd.DataFrame(records_sorted)
                            upload_ok = upload_parquet_to_hdfs(df, act["target_dir"])
                            if not upload_ok:
                                print(f"[!] [HDFS Archiver] Upload '{subzone}' (P{part_id}, batch {act['min_offset']}_{act['max_offset']}) thất bại. Giữ nguyên batch để retry.", flush=True)
                                continue

                            # Upload Parquet thành công!
                            act["uploaded"] = True
                            print(f"[+] [HDFS Archiver] Đã lưu {len(records_sorted)} bản ghi '{subzone}' (P{part_id}, batch {act['min_offset']}_{act['max_offset']}) vào: {act['target_dir']}", flush=True)

                            # Cập nhật trạng thái uploaded vào manifest
                            manifest["pending_batches"][topic_name][str(part_id)]["uploaded"] = True
                            manifest_saved = save_manifest(manifest)
                            if not manifest_saved:
                                print(f"[!] [HDFS Archiver] Lưu manifest uploaded cho P{part_id} thất bại. Sẽ thử lại lưu manifest trước khi commit.", flush=True)
                                continue

                        # Đảm bảo manifest đã ghi nhận uploaded=True trước khi commit Kafka
                        if not manifest.get("pending_batches", {}).get(topic_name, {}).get(str(part_id), {}).get("uploaded", False):
                            manifest.setdefault("pending_batches", {}).setdefault(topic_name, {}).setdefault(str(part_id), {})["uploaded"] = True
                            if not save_manifest(manifest):
                                print(f"[!] [HDFS Archiver] Đồng bộ manifest uploaded cho P{part_id} thất bại. Dừng trước khi commit.", flush=True)
                                continue

                        # Bước 2.3: Commit offset lên Kafka
                        tp_obj = TopicPartition(topic_name, part_id)
                        max_off = act["max_offset"]
                        commit_dict = {tp_obj: OffsetAndMetadata(max_off + 1, "")}
                        commit_ok = True
                        try:
                            consumer.commit(commit_dict)
                        except Exception as e_comm:
                            commit_ok = False
                            print(f"[!] [HDFS Archiver] Commit offset {max_off + 1} cho '{topic_name}':P{part_id} thất bại: {e_comm}. Giữ nguyên batch {act['min_offset']}_{max_off} để retry.", flush=True)

                        if not commit_ok:
                            continue

                        # Bước 2.4: Commit Kafka thành công -> Cập nhật manifest chính thức & gỡ bỏ pending batch
                        if topic_name not in manifest.setdefault("topics", {}):
                            manifest["topics"][topic_name] = {}
                        manifest["topics"][topic_name][str(part_id)] = max(
                            manifest["topics"][topic_name].get(str(part_id), -1),
                            max_off
                        )
                        if topic_name in manifest.get("pending_batches", {}) and str(part_id) in manifest["pending_batches"][topic_name]:
                            del manifest["pending_batches"][topic_name][str(part_id)]

                        manifest_saved = save_manifest(manifest)
                        if not manifest_saved:
                            print(f"[!] [HDFS Archiver] Lưu manifest hoàn tất cho P{part_id} thất bại. Giữ lại batch để retry lưu manifest.", flush=True)
                            continue

                        # Batch đã hoàn tất 100% không thể ghi trùng lặp
                        print(f"[+] [HDFS Archiver] Hoàn tất & commit bền vững batch {act['min_offset']}_{max_off} cho P{part_id}!", flush=True)
                        del act_dict[part_id]

                last_flush_time = now

    except KeyboardInterrupt:
        print("\n[*] [HDFS Archiver] Dừng tiến trình lưu trữ.", flush=True)
    finally:
        if consumer:
            try:
                consumer.close()
            except Exception:
                pass


if __name__ == '__main__':
    run_hdfs_archiver()
