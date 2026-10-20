#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
scratch/measure_latency.py
==========================
Đo độ trễ đầu-cuối (End-to-End Latency) của luồng Lambda / Structured Streaming:
Producer (tourism-input) -> Spark Structured Streaming -> Consumer (tourism-output).

Số liệu đo lường:
- N (mặc định 50) reviews hợp lệ gửi vào tourism-input kèm sent_at_ms và event_id.
- Đọc từ tourism-output, đối chiếu event_id, tính latency = received_at_ms - sent_at_ms.
- Tính toán percentile: p50 (median), p95, max, min, avg.
- Ghi kết quả thật vào data/evidence/latency_<timestamp>.json (KHÔNG bịa số liệu).
"""

import sys
import os
import json
import time
import argparse
import uuid
from datetime import datetime

# Đảm bảo UTF-8 trên console Windows
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')

# Thêm root vào sys.path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

try:
    from kafka import KafkaProducer, KafkaConsumer
except ImportError:
    print("[ERROR] Thư viện kafka-python chưa được cài đặt. Hãy chạy: pip install kafka-python")
    sys.exit(1)


def parse_args():
    parser = argparse.ArgumentParser(description="Đo độ trễ End-to-End của Spark Structured Streaming")
    parser.add_argument("--count", "-n", type=int, default=50, help="Số lượng sự kiện gửi kiểm thử (mặc định: 50)")
    parser.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"), help="Kafka Bootstrap Server")
    parser.add_argument("--input-topic", default="tourism-input", help="Topic đầu vào")
    parser.add_argument("--output-topic", default="tourism-output", help="Topic đầu ra")
    parser.add_argument("--timeout", type=int, default=60, help="Thời gian chờ tối đa (giây) cho consumer (mặc định: 60s)")
    return parser.parse_args()


def calculate_percentiles(latencies):
    if not latencies:
        return {}
    sorted_lats = sorted(latencies)
    n = len(sorted_lats)
    
    def percentile(p):
        k = (n - 1) * p
        f = int(k)
        c = f + 1
        if c < n:
            return sorted_lats[f] + (k - f) * (sorted_lats[c] - sorted_lats[f])
        return sorted_lats[f]

    return {
        "count": n,
        "min_ms": round(sorted_lats[0], 2),
        "p50_ms": round(percentile(0.50), 2),
        "p90_ms": round(percentile(0.90), 2),
        "p95_ms": round(percentile(0.95), 2),
        "max_ms": round(sorted_lats[-1], 2),
        "avg_ms": round(sum(sorted_lats) / n, 2)
    }


def main():
    args = parse_args()
    n_events = args.count
    bootstrap_server = args.bootstrap
    input_topic = args.input_topic
    output_topic = args.output_topic
    timeout_sec = args.timeout

    print("=" * 70)
    print(" BẮT ĐẦU ĐO ĐỘ TRỄ ĐẦU-CUỐI (MEASURE STREAMING LATENCY)")
    print(f" - Bootstrap: {bootstrap_server}")
    print(f" - Input Topic: {input_topic} -> Output Topic: {output_topic}")
    print(f" - Số lượng sự kiện cần đo: {n_events}")
    print(f" - Thời hạn chờ tối đa: {timeout_sec}s")
    print("=" * 70)

    # 1. Khởi tạo Consumer trước để lắng nghe các message mới
    consumer_group = f"latency_measurer_{int(time.time())}"
    try:
        consumer = KafkaConsumer(
            output_topic,
            bootstrap_servers=bootstrap_server,
            group_id=consumer_group,
            auto_offset_reset='latest',
            enable_auto_commit=True,
            value_deserializer=lambda m: json.loads(m.decode('utf-8', errors='ignore')),
            consumer_timeout_ms=1000  # poll timeout 1s
        )
    except Exception as e:
        print(f"[FATAL] Không thể kết nối Kafka Consumer: {e}")
        print("Vui lòng đảm bảo Kafka Broker đang chạy tại port 9092.")
        sys.exit(1)

    # 2. Khởi tạo Producer
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_server,
            acks='all',
            retries=3,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8')
        )
    except Exception as e:
        print(f"[FATAL] Không thể khởi tạo Kafka Producer: {e}")
        consumer.close()
        sys.exit(1)

    # Chuẩn bị danh sách sự kiện
    sent_events = {}
    print(f"\n[1/3] Đang gửi {n_events} sự kiện vào topic '{input_topic}'...")
    start_send_ts = time.time()

    sample_spots = [
        {"stt": 1, "name": "Bà Nà Hills", "location": "Đà Nẵng", "lat": 15.998, "lng": 107.996},
        {"stt": 2, "name": "Hồ Gươm", "location": "Hà Nội", "lat": 21.028, "lng": 105.854},
        {"stt": 3, "name": "Chợ Bến Thành", "location": "Hồ Chí Minh", "lat": 10.772, "lng": 106.698},
        {"stt": 4, "name": "VinWonders Phú Quốc", "location": "Kiên Giang", "lat": 10.334, "lng": 103.864},
        {"stt": 5, "name": "Vịnh Hạ Long", "location": "Quảng Ninh", "lat": 20.910, "lng": 107.183}
    ]

    for i in range(n_events):
        spot = sample_spots[i % len(sample_spots)]
        event_id = f"latency_test_{int(time.time()*1000)}_{i}_{uuid.uuid4().hex[:6]}"
        now_ms = time.time() * 1000.0

        payload = {
            "event_id": event_id,
            "session_id": f"sess_latency_{i % 5}",
            "source": "latency_test_runner",
            "stt": spot["stt"],
            "name": spot["name"],
            "spot_name": spot["name"],
            "rating": 4.5,
            "user_name": f"Tester_{i+1}",
            "review_text": f"Đo kiểm độ trễ tự động đợt {i+1}",
            "latitude": spot["lat"],
            "longitude": spot["lng"],
            "location": spot["location"],
            "keywords": ["du lịch", "đo kiểm", "latency"],
            "sent_at_ms": now_ms,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }

        sent_events[event_id] = now_ms
        fut = producer.send(input_topic, key=payload["session_id"].encode('utf-8'), value=payload)
        fut.get(timeout=5)  # Đảm bảo đã gửi xong

    producer.flush()
    print(f"[+] Đã gửi xong {len(sent_events)} sự kiện trong {time.time() - start_send_ts:.2f}s.")

    # 3. Lắng nghe và thu thập độ trễ từ tourism-output
    print(f"\n[2/3] Đang lắng nghe kết quả từ topic '{output_topic}' (chờ tối đa {timeout_sec}s)...")
    latencies = []
    received_details = []
    listen_start = time.time()

    while time.time() - listen_start < timeout_sec:
        records = consumer.poll(timeout_ms=1000)
        for tp, msgs in records.items():
            recv_time_ms = time.time() * 1000.0
            for msg in msgs:
                val = msg.value
                if not isinstance(val, dict):
                    continue
                ev_id = val.get("event_id")
                if ev_id in sent_events:
                    sent_ms = sent_events[ev_id]
                    latency_ms = recv_time_ms - sent_ms
                    latencies.append(latency_ms)
                    received_details.append({
                        "event_id": ev_id,
                        "sent_at_ms": sent_ms,
                        "received_at_ms": recv_time_ms,
                        "latency_ms": round(latency_ms, 2),
                        "cluster_assigned": val.get("ml_analysis", {}).get("kmeans", {}).get("cluster_name")
                    })
                    print(f"  -> Nhận #{len(latencies)}/{n_events}: {ev_id} | Latency: {latency_ms:.1f} ms")

        if len(latencies) >= n_events:
            print("[+] Đã nhận đủ toàn bộ sự kiện đã gửi!")
            break

    consumer.close()
    producer.close()

    # 4. Tính toán số liệu thống kê
    print("\n[3/3] Kết quả phân tích thống kê độ trễ:")
    stats = calculate_percentiles(latencies)
    if not stats:
        print("[!] Không nhận được phản hồi nào từ Spark Structured Streaming trong thời gian chờ!")
        print("    Vui lòng kiểm tra lại xem job spark_structured_streaming.py đã chạy chưa.")
        sys.exit(1)

    print("-" * 50)
    print(f"  • Tổng số gửi:      {n_events}")
    print(f"  • Số sự kiện nhận:  {stats['count']} ({stats['count']/n_events*100:.1f}%)")
    print(f"  • Độ trễ Min:       {stats['min_ms']} ms")
    print(f"  • Độ trễ Trung bình: {stats['avg_ms']} ms")
    print(f"  • Độ trễ p50:       {stats['p50_ms']} ms")
    print(f"  • Độ trễ p90:       {stats['p90_ms']} ms")
    print(f"  • Độ trễ p95:       {stats['p95_ms']} ms")
    print(f"  • Độ trễ Max:       {stats['max_ms']} ms")
    print("-" * 50)

    # 5. Ghi bằng chứng thật vào data/evidence/latency_<timestamp>.json
    evidence_dir = os.path.join(PROJECT_ROOT, "data", "evidence")
    os.makedirs(evidence_dir, exist_ok=True)
    out_filename = f"latency_{int(time.time())}.json"
    out_path = os.path.join(evidence_dir, out_filename)

    evidence_data = {
        "benchmark": "Streaming End-to-End Latency",
        "timestamp": datetime.now().isoformat(),
        "total_sent": n_events,
        "total_received": stats["count"],
        "statistics": stats,
        "events": received_details
    }

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(evidence_data, f, ensure_ascii=False, indent=2)

    print(f"[+] Bằng chứng số liệu đã được lưu thành công tại:\n    {out_path}")
    print("=" * 70)


if __name__ == "__main__":
    main()
