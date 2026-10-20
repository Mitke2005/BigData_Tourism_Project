import os
import sys
import time
import json
import uuid
import random
import argparse
import pandas as pd
from datetime import datetime

if sys.platform.startswith('win'):
    try:
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
        sys.stderr.reconfigure(encoding='utf-8', errors='replace')
    except Exception:
        pass

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)

try:
    from kafka import KafkaProducer, KafkaConsumer  # type: ignore
except ImportError:
    from kafka_python_ng import KafkaProducer, KafkaConsumer  # type: ignore


def clean_keyword_str(val):
    if not val:
        return ""
    return str(val).replace('"', '').replace("'", "").replace('“', '').replace('”', '').strip().lower()


def extract_clean_keywords(raw_val):
    if not raw_val:
        return []
    if isinstance(raw_val, list):
        items = raw_val
    else:
        items = str(raw_val).split(',')
    cleaned = []
    seen = set()
    for item in items:
        w = clean_keyword_str(item)
        if w and w not in seen:
            seen.add(w)
            cleaned.append(w)
    return cleaned


def wait_for_streaming_heartbeat(bootstrap_servers="localhost:9092", control_topic="tourism-control", timeout_sec=60):
    """
    Lắng nghe tourism-control để biết Spark Streaming đã ready (nhận message chứa streaming:heartbeat).
    Nếu quá 60s chưa có heartbeat -> raise TimeoutError rõ ràng.
    """
    print(f"[*] Đang lắng nghe tín hiệu 'streaming:heartbeat' trên topic '{control_topic}' (chờ tối đa {timeout_sec}s)...")
    start_time = time.time()
    consumer = None
    try:
        consumer = KafkaConsumer(
            control_topic,
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset='latest',
            enable_auto_commit=False,
            consumer_timeout_ms=1000,
            api_version=(2, 8, 1)
        )
        while time.time() - start_time < timeout_sec:
            records = consumer.poll(timeout_ms=1000)
            for tp, msgs in records.items():
                for msg in msgs:
                    key_str = (msg.key.decode('utf-8', errors='ignore') if msg.key else "")
                    val_str = (msg.value.decode('utf-8', errors='ignore') if msg.value else "")
                    if "streaming:heartbeat" in key_str or "streaming:heartbeat" in val_str or "STREAMING_HEARTBEAT" in val_str:
                        print(f"[+] Nhận được heartbeat từ Spark Structured Streaming sau {round(time.time() - start_time, 2)}s! Bắt đầu phát dữ liệu.")
                        return True
            elapsed = int(time.time() - start_time)
            if elapsed > 0 and elapsed % 10 == 0:
                print(f"[*] Vẫn đang chờ Spark Streaming heartbeat... ({elapsed}/{timeout_sec}s)")
        
        raise TimeoutError(
            f"Quá {timeout_sec}s chưa nhận được tín hiệu 'streaming:heartbeat' từ topic '{control_topic}'. "
            "Vui lòng đảm bảo Spark Structured Streaming ('spark_structured_streaming.py') đang chạy ổn định."
        )
    finally:
        if consumer:
            try:
                consumer.close()
            except Exception:
                pass


def run_producer(csv_path=None,
                 bootstrap_servers="localhost:9092",
                 topic="tourism-input",
                 control_topic="tourism-control",
                 min_delay=1.0,
                 max_delay=3.0,
                 wait_for_streaming=True):
    
    if csv_path is None:
        csv_path = os.path.join(BASE_DIR, "data", "tourism_dataset_clean.csv")

    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Không tìm thấy tập dữ liệu danh mục tại: {csv_path}")

    # 1. Chờ heartbeat từ Spark Structured Streaming nếu được yêu cầu
    if wait_for_streaming:
        wait_for_streaming_heartbeat(
            bootstrap_servers=bootstrap_servers,
            control_topic=control_topic,
            timeout_sec=60
        )

    # 2. Khởi tạo Kafka Producer
    print(f"[*] Đang kết nối Kafka Producer tới: {bootstrap_servers}...")
    producer = None
    for attempt in range(1, 10):
        try:
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
                api_version=(2, 8, 1)
            )
            print("[+] Kết nối Kafka Producer thành công!")
            break
        except Exception as e:
            print(f"[-] Thử lại ({attempt}/10) kết nối Kafka sau 2s... Lỗi: {e}")
            time.sleep(2)

    if not producer:
        raise ConnectionError(f"Không thể kết nối tới Kafka Broker tại {bootstrap_servers}!")

    print(f"[*] Đang đọc tập dữ liệu từ: {csv_path}")
    df = pd.read_csv(csv_path)
    total_records = len(df)
    print(f"[+] Đã tải {total_records} địa điểm du lịch từ master dataset.")
    print(f"[*] Bắt đầu phát luồng sự kiện SIMULATED_VIEW (delay {min_delay}-{max_delay}s) vào topic '{topic}'...")

    event_count = 0
    cycle = 1

    try:
        while True:
            for idx, row in df.iterrows():
                event_count += 1
                stt_val = int(row['STT'])
                name_val = str(row['Tên địa điểm']).strip()
                clean_kws = extract_clean_keywords(row.get('Từ Khóa (Mở rộng)', row.get('Từ Khóa', '')))

                # QUY TẮC BẮT BUỘC: Event SIMULATED_VIEW KHÔNG được làm đổi rating của spot
                # rating = None, is_user_review = False, event_type = "SIMULATED_VIEW"
                payload = {
                    "event_id": str(uuid.uuid4()),
                    "session_id": f"sess_{stt_val % 6}",
                    "event_type": "SIMULATED_VIEW",
                    "spot_id": stt_val,
                    "stt": stt_val,
                    "name": name_val,
                    "location": str(row['Vị trí']).strip(),
                    "description": str(row.get('Mô tả', '')).strip(),
                    "rating": None,
                    "is_user_review": False,
                    "source": "simulated_producer",
                    "cycle": cycle,
                    "image": str(row.get('Ảnh', '')).strip() if pd.notna(row.get('Ảnh', '')) else "",
                    "keywords": clean_kws,
                    "latitude": float(row['Latitude']) if pd.notna(row.get('Latitude')) else 0.0,
                    "longitude": float(row['Longitude']) if pd.notna(row.get('Longitude')) else 0.0,
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }

                partition_key = payload["session_id"].encode('utf-8')
                fut = producer.send(topic, key=partition_key, value=payload)
                fut.get(timeout=3.0)

                delay = random.uniform(min_delay, max_delay)
                print(f"[{payload['timestamp']}] #Event {event_count} [Key: {payload['session_id']}] -> SIMULATED_VIEW: #{stt_val} {name_val} (Next delay: {round(delay, 2)}s)")
                time.sleep(delay)

            cycle += 1
            print(f"[*] Đã hoàn tất chu kỳ phát mô phỏng {cycle - 1}. Lặp lại vòng lặp tiếp theo...")

    except KeyboardInterrupt:
        print("\n[!] Dừng Kafka Producer theo yêu cầu người dùng.")
    finally:
        producer.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Vietnam Tourism Kafka Event Producer")
    parser.add_argument("--csv-path", type=str, default=None, help="Đường dẫn file CSV danh mục master")
    parser.add_argument("--bootstrap-servers", type=str, default="localhost:9092", help="Kafka bootstrap servers")
    parser.add_argument("--topic", type=str, default="tourism-input", help="Topic nhận dữ liệu đầu vào")
    parser.add_argument("--control-topic", type=str, default="tourism-control", help="Topic điều khiển lắng nghe heartbeat")
    parser.add_argument("--min-delay", type=float, default=1.0, help="Thời gian chờ tối thiểu giữa 2 events (giây)")
    parser.add_argument("--max-delay", type=float, default=3.0, help="Thời gian chờ tối đa giữa 2 events (giây)")
    parser.add_argument("--no-wait-streaming", action="store_true", help="Bỏ qua chờ heartbeat từ Spark Structured Streaming")

    args = parser.parse_args()

    run_producer(
        csv_path=args.csv_path,
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        control_topic=args.control_topic,
        min_delay=args.min_delay,
        max_delay=args.max_delay,
        wait_for_streaming=not args.no_wait_streaming
    )

