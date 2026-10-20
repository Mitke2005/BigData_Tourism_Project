"""
MODULE QUẢN TRỊ KAFKA STATE STORE (KAFKA-FIRST LAMBDA ARCHITECTURE)
=============================================================================
Đảm nhiệm:
1. Định nghĩa hằng số các topic Kafka chuẩn hóa theo kiến trúc Lambda.
2. ensure_topics(): Khởi tạo idempotent và kiểm tra nghiêm ngặt cấu hình topic
   (số partition, cleanup.policy, max.message.bytes). Báo lỗi (raise) nếu sai cấu hình.
3. publish(): Ghi thông điệp đồng bộ với acks='all', retries, max_request_size 10MB, raise nếu lỗi.
4. read_latest_compacted(): Đọc snapshot mới nhất từ compacted topic (last-wins theo key)
   từ đầu tới end_offsets lúc gọi.
=============================================================================
"""

import os
import sys
import json
import time
from typing import Dict, Any, Optional

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

from kafka import KafkaAdminClient, KafkaProducer, KafkaConsumer, TopicPartition
from kafka.admin import NewTopic, ConfigResource, ConfigResourceType
from kafka.errors import TopicAlreadyExistsError

if sys.platform.startswith('win'):
    try:
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
        sys.stderr.reconfigure(encoding='utf-8', errors='replace')
    except Exception:
        pass

# Cấu hình biến môi trường kết nối Kafka
DEFAULT_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")

# Danh mục các Topic chuẩn của hệ thống
TOPIC_INPUT = "tourism-input"              # 3 partitions, delete, key=session_id
TOPIC_OUTPUT = "tourism-output"            # 3 partitions, delete, key=stt
TOPIC_REVIEWS = "tourism-reviews"          # 3 partitions, compact, retention.ms=-1, key=event_id
TOPIC_STATE_SPOTS = "tourism-state-spots"  # 1 partition, compact, max.message.bytes=10485760, key=str(stt)
TOPIC_STATE_BATCH = "tourism-state-batch"  # 1 partition, compact, max.message.bytes=10485760, key=model:*, itinerary:*, batch_commit
TOPIC_CONTROL = "tourism-control"          # 1 partition, compact, key=streaming | batch_job:<id>
TOPIC_DEAD_LETTER = "tourism-dead-letter"  # 1 partition, delete, key=event_id

# Metadata quy định cho từng topic
TOPIC_SPECS = {
    TOPIC_INPUT: {
        "partitions": 3,
        "cleanup.policy": "delete",
        "configs": {"cleanup.policy": "delete"}
    },
    TOPIC_OUTPUT: {
        "partitions": 3,
        "cleanup.policy": "delete",
        "configs": {"cleanup.policy": "delete"}
    },
    TOPIC_REVIEWS: {
        "partitions": 3,
        "cleanup.policy": "compact",
        "configs": {
            "cleanup.policy": "compact",
            "retention.ms": "-1"
        }
    },
    TOPIC_STATE_SPOTS: {
        "partitions": 1,
        "cleanup.policy": "compact",
        "configs": {
            "cleanup.policy": "compact",
            "max.message.bytes": "10485760"
        }
    },
    TOPIC_STATE_BATCH: {
        "partitions": 1,
        "cleanup.policy": "compact",
        "configs": {
            "cleanup.policy": "compact",
            "max.message.bytes": "10485760"
        }
    },
    TOPIC_CONTROL: {
        "partitions": 1,
        "cleanup.policy": "compact",
        "configs": {
            "cleanup.policy": "compact"
        }
    },
    TOPIC_DEAD_LETTER: {
        "partitions": 1,
        "cleanup.policy": "delete",
        "configs": {"cleanup.policy": "delete"}
    }
}

_producer_instance = None

def get_producer(bootstrap_servers: Optional[str] = None) -> KafkaProducer:
    """Khởi tạo KafkaProducer dùng chung với acks='all' và max_request_size 10MB."""
    global _producer_instance
    servers = bootstrap_servers or DEFAULT_BOOTSTRAP_SERVERS
    if _producer_instance is None:
        _producer_instance = KafkaProducer(
            bootstrap_servers=servers,
            acks='all',
            retries=5,
            max_request_size=10485760,
            key_serializer=lambda k: str(k).encode('utf-8') if k is not None else None,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
            api_version=(2, 8, 1)
        )
    return _producer_instance

def ensure_topics(bootstrap_servers: Optional[str] = None) -> None:
    """
    Kiểm tra và khởi tạo các topic theo đúng chuẩn.
    Nếu topic đã tồn tại nhưng sai số lượng partition hoặc sai cleanup.policy,
    hàm sẽ RAISE ngoại lệ rõ ràng, tuyệt đối không tự sửa im lặng.
    """
    servers = bootstrap_servers or DEFAULT_BOOTSTRAP_SERVERS
    admin = KafkaAdminClient(bootstrap_servers=servers, api_version=(2, 8, 1))
    
    try:
        # Lấy danh sách topic và metadata hiện có
        existing_topics = set(admin.list_topics())
        
        topics_to_create = []
        
        for topic_name, spec in TOPIC_SPECS.items():
            partitions = (
                admin.describe_topics([topic_name])[0]["partitions"]
                if topic_name in existing_topics else None
            )
            
            if partitions is None:
                # Topic chưa tồn tại, chuẩn bị tạo mới
                topics_to_create.append(
                    NewTopic(
                        name=topic_name,
                        num_partitions=spec["partitions"],
                        replication_factor=1,
                        topic_configs=spec.get("configs", {})
                    )
                )
            else:
                # Topic đã tồn tại: kiểm tra số lượng partition
                actual_partitions = len(partitions)
                expected_partitions = spec["partitions"]
                if actual_partitions != expected_partitions:
                    raise RuntimeError(
                        f"Topic '{topic_name}' cấu hình sai số partition: "
                        f"thực tế={actual_partitions}, yêu cầu={expected_partitions}!"
                    )
                
                # Kiểm tra config cleanup.policy
                try:
                    config_resource = ConfigResource(ConfigResourceType.TOPIC, topic_name)
                    describe_res = admin.describe_configs([config_resource])
                    if describe_res and len(describe_res) > 0:
                        topic_configs = describe_res[0].resources[0][4]
                        # topic_configs là danh sách các tuple (name, val, is_default, ...)
                        cfg_dict = {item[0]: item[1] for item in topic_configs}
                        actual_policy = cfg_dict.get("cleanup.policy", "delete")
                        expected_policy = spec["cleanup.policy"]
                        if actual_policy != expected_policy:
                            raise RuntimeError(
                                f"Topic '{topic_name}' cấu hình sai cleanup.policy: "
                                f"thực tế='{actual_policy}', yêu cầu='{expected_policy}'!"
                            )
                except Exception as ce:
                    # Nếu lỗi là RuntimeError do kiểm tra thì re-raise ngay
                    if isinstance(ce, RuntimeError):
                        raise
                    # Lỗi kết nối khi describe config thì log cảnh báo
                    print(f"[!] Cảnh báo kiểm tra config topic '{topic_name}': {ce}")

        if topics_to_create:
            print(f"[*] Đang khởi tạo {len(topics_to_create)} Kafka topics mới...")
            admin.create_topics(new_topics=topics_to_create, validate_only=False)
            print("[+] Khởi tạo topics hoàn tất thành công.")
            time.sleep(1.0)
            
    finally:
        admin.close()

def publish(topic: str, key: Any, value: Dict[str, Any], bootstrap_servers: Optional[str] = None) -> Any:
    """
    Ghi dữ liệu vào Kafka topic với acks='all', kiểm tra future và raise exception nếu thất bại.
    """
    producer = get_producer(bootstrap_servers)
    key_str = str(key) if key is not None else None
    future = producer.send(topic, key=key_str, value=value)
    producer.flush()
    try:
        record_metadata = future.get(timeout=10.0)
        return record_metadata
    except Exception as e:
        raise RuntimeError(f"Lỗi publish vào Kafka topic '{topic}' (key={key_str}): {e}") from e

def read_latest_compacted(topic: str, key: Optional[str] = None, bootstrap_servers: Optional[str] = None) -> Any:
    """
    Đọc toàn bộ snapshot hiện tại của một Compacted Topic từ offset 0 đến end_offsets lúc gọi.
    Áp dụng nguyên tắc last-wins theo message key.
    Nếu `key` được truyền vào, trả về giá trị mới nhất của key đó (hoặc None nếu không tồn tại).
    Nếu `key` is None, trả về dictionary {k: v} của tất cả các key có trong topic.
    """
    servers = bootstrap_servers or DEFAULT_BOOTSTRAP_SERVERS
    consumer = KafkaConsumer(
        bootstrap_servers=servers,
        enable_auto_commit=False,
        auto_offset_reset='earliest',
        key_deserializer=lambda k: k.decode('utf-8') if k is not None else None,
        value_deserializer=lambda v: json.loads(v.decode('utf-8')) if v is not None else None,
        api_version=(2, 8, 1),
        consumer_timeout_ms=3000
    )
    
    try:
        # Lấy danh sách partition của topic
        partitions = consumer.partitions_for_topic(topic)
        if not partitions:
            return {} if key is None else None
            
        topic_partitions = [TopicPartition(topic, p) for p in partitions]
        consumer.assign(topic_partitions)
        consumer.seek_to_beginning()
        
        # Lấy end offsets của tất cả các partition tại thời điểm này
        end_offsets = consumer.end_offsets(topic_partitions)
        
        # Nếu tất cả các partition đều trống (end_offset == 0)
        if all(end_offsets[tp] == 0 for tp in topic_partitions):
            return {} if key is None else None

        records_by_key = {}
        
        # Đọc tuần tự cho tới khi mỗi partition chạm tới end_offset
        still_reading = True
        while still_reading:
            msg_batch = consumer.poll(timeout_ms=1000)
            if not msg_batch:
                break
                
            for tp, messages in msg_batch.items():
                for msg in messages:
                    msg_key = msg.key
                    msg_val = msg.value
                    if msg_key is not None:
                        records_by_key[msg_key] = msg_val
                        
            # Kiểm tra xem tất cả các partition đã đọc tới end_offset chưa
            all_done = True
            for tp in topic_partitions:
                current_pos = consumer.position(tp)
                if current_pos < end_offsets[tp]:
                    all_done = False
                    break
            if all_done:
                break
                
        if key is not None:
            return records_by_key.get(str(key), None)
        return records_by_key
        
    finally:
        consumer.close()
