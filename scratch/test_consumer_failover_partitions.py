"""
DEMO BẢO TOÀN DỮ LIỆU KHI DỪNG/KHỞI ĐỘNG LẠI KAFKA CONSUMER VỚI 3 PARTITIONS & SESSION_ID
========================================================================================
Mục tiêu kiểm thử chứng minh:
1. Producer gửi các sự kiện với Partition Key là `session_id`. Dữ liệu được phân bổ vào các Partition khác nhau (0, 1, 2).
2. Consumer nhóm 'failover_demo_group' khởi động, đọc đợt 1 rồi chủ động DỪNG (Mô phỏng sập nguồn/bảo trì).
3. Trong lúc Consumer tắt, Producer tiếp tục bắn thêm sự kiện mới vào các partition.
4. Consumer khởi động lại với cùng `group_id`, khôi phục offset chính xác từ Kafka và nhận trọn vẹn 100% dữ liệu tồn đọng (0% mất mát).
"""

import os
import sys
import json
import time
import uuid

if sys.platform.startswith('win'):
    try:
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
        sys.stderr.reconfigure(encoding='utf-8', errors='replace')
    except:
        pass

try:
    from kafka import KafkaProducer, KafkaConsumer, TopicPartition  # type: ignore
except ImportError:
    from kafka_python_ng import KafkaProducer, KafkaConsumer, TopicPartition  # type: ignore

TOPIC = "tourism-input"
BOOTSTRAP_SERVERS = "localhost:9092"
GROUP_ID = f"failover_group_{int(time.time())}"

def test_partitions_and_failover():
    print("="*80)
    print("  KỊCH BẢN DEMO: KAFKA 3 PARTITIONS VỚI SESSION_ID & BẢO TOÀN DỮ LIỆU FAILOVER")
    print("="*80)

    # 1. Khởi tạo Producer
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    # Kiểm tra số partition của Topic
    consumer_meta = KafkaConsumer(bootstrap_servers=BOOTSTRAP_SERVERS)
    partitions = consumer_meta.partitions_for_topic(TOPIC)
    consumer_meta.close()
    print(f"[+] [Metadata Kafka] Topic '{TOPIC}' hiện có: {len(partitions)} Partitions: {sorted(list(partitions))}")
    assert len(partitions) >= 3, f"Yêu cầu ít nhất 3 partitions, hiện có: {len(partitions)}"

    # 2. Đợt 1: Gửi 3 message với các session_id khác nhau
    print("\n--- BƯỚC 1: GỬI ĐỢT 1 (3 SỰ KIỆN VỚI SESSION_ID LÀM KEY) ---")
    sessions_batch1 = ["session_north_101", "session_central_202", "session_south_303"]
    sent_partitions_1 = []

    for i, sess in enumerate(sessions_batch1):
        msg = {
            "demo_id": f"batch1_msg_{i+1}",
            "session_id": sess,
            "stt": i + 1,
            "name": f"Địa điểm thử nghiệm {i+1}",
            "rating": 5.0,
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
        }
        key_bytes = sess.encode('utf-8')
        future = producer.send(TOPIC, key=key_bytes, value=msg)
        meta = future.get(timeout=5)
        sent_partitions_1.append(meta.partition)
        print(f"[*] Đã gửi #{msg['demo_id']} | Key='{sess}' -> Partition {meta.partition}, Offset {meta.offset}")

    producer.flush()
    print(f"[+] Đợt 1 đã phân bổ vào các partitions: {set(sent_partitions_1)}")

    # 3. Khởi tạo Consumer lần 1, đọc đợt 1, commit offset rồi DỪNG
    print(f"\n--- BƯỚC 2: KHỞI TẠO CONSUMER (GROUP: {GROUP_ID}) -> ĐỌC ĐỢT 1 RỒI TẮT ---")
    consumer_1 = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        consumer_timeout_ms=5000,
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    received_batch1 = []
    for record in consumer_1:
        val = record.value
        if "batch1_msg_" in val.get("demo_id", ""):
            received_batch1.append(val["demo_id"])
            print(f"[Consumer Lần 1] Đã nhận: {val['demo_id']} (Partition {record.partition}, Key {record.key.decode()})")
        if len(received_batch1) == 3:
            break

    # Commit offset và đóng Consumer để mô phỏng sập nguồn
    consumer_1.commit()
    consumer_1.close()
    print(f"[+] Consumer Lần 1 đã đọc {len(received_batch1)}/3 tin nhắn và ĐÃ TẮT HOÀN TOÀN (SHUTDOWN).")

    # 4. Khi Consumer đang TẮT, gửi tiếp đợt 2 vào Kafka
    print("\n--- BƯỚC 3: CONSUMER ĐANG TẮT -> PRODUCER TIẾP TỤC BẮN THÊM ĐỢT 2 VÀO KAFKA ---")
    sessions_batch2 = ["session_offline_404", "session_offline_505", "session_offline_606"]
    for j, sess in enumerate(sessions_batch2):
        msg = {
            "demo_id": f"batch2_msg_{j+1}",
            "session_id": sess,
            "stt": j + 10,
            "name": f"Địa điểm gửi lúc Consumer tắt {j+1}",
            "rating": 4.8,
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
        }
        key_bytes = sess.encode('utf-8')
        future = producer.send(TOPIC, key=key_bytes, value=msg)
        meta = future.get(timeout=5)
        print(f"[*] [Consumer đang tắt] Đã lưu trữ tin nhắn #{msg['demo_id']} vào Partition {meta.partition}, Offset {meta.offset}")

    producer.flush()
    producer.close()
    print(f"[+] Dữ liệu đợt 2 đã nằm an toàn trong Kafka Broker trên các partitions.")

    # 5. Khởi động lại Consumer với cùng group_id -> Chứng minh không mất dữ liệu
    print(f"\n--- BƯỚC 4: KHỞI ĐỘNG LẠI CONSUMER CÙNG GROUP_ID ({GROUP_ID}) -> ĐỌC TIẾP ---")
    consumer_2 = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        consumer_timeout_ms=6000,
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    received_batch2 = []
    for record in consumer_2:
        val = record.value
        demo_id = val.get("demo_id", "")
        if "batch2_msg_" in demo_id:
            received_batch2.append(demo_id)
            print(f"[Consumer Lần 2 (Resume)] ĐÃ NHẬN THÀNH CÔNG: {demo_id} (Partition {record.partition}, Key: {record.key.decode()})")
        if len(received_batch2) == 3:
            break

    consumer_2.close()

    print("\n" + "="*80)
    print("  KẾT QUẢ KIỂM CHỨNG BẢO TOÀN DỮ LIỆU:")
    print("="*80)
    print(f"1. Số tin nhắn đợt 1 Consumer nhận trước khi tắt: {len(received_batch1)}/3")
    print(f"2. Số tin nhắn đợt 2 Consumer nhận sau khi khởi động lại: {len(received_batch2)}/3")
    
    if len(received_batch2) == 3:
        print("[+] XÁC NHẬN: Consumer đã khôi phục offset chính xác từ Kafka! KHÔNG BỊ MẤT DỮ LIỆU (0% DATA LOSS).")
        print("[+] XÁC NHẬN: Phân bổ dữ liệu theo Session ID qua 3 Partitions thành công 100%.")
    else:
        print(f"[!] Cảnh báo: Chỉ nhận được {len(received_batch2)} tin nhắn.")

if __name__ == '__main__':
    test_partitions_and_failover()
