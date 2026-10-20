import os
import sys
import json
import time
from datetime import datetime
import pandas as pd

if sys.platform.startswith('win'):
    try:
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
        sys.stderr.reconfigure(encoding='utf-8', errors='replace')
    except:
        pass

try:
    from kafka import KafkaProducer  # type: ignore
except ImportError:
    from kafka_python_ng import KafkaProducer  # type: ignore


def send_live_review(spot_name="Bà Nà Hills", rating=5.0, user_name="Nguyễn Trường Nhu (Sinh viên HUIT)", review_text="Trải nghiệm tuyệt vời, không khí trong lành, dịch vụ đẳng cấp 5 sao!"):
    csv_path = "D:/BigData/DOAN/data/tourism_dataset_clean.csv"
    lat, lng, loc, kw, img, desc = 15.998, 107.986, "Đà Nẵng", ["du lịch", "cảnh đẹp", "nghỉ dưỡng"], "", review_text
    
    if os.path.exists(csv_path):
        df = pd.read_csv(csv_path)
        match = df[df['Tên địa điểm'].str.contains(spot_name, case=False, na=False)]
        if not match.empty:
            row = match.iloc[0]
            spot_name = row['Tên địa điểm']
            lat = float(row['Latitude'])
            lng = float(row['Longitude'])
            loc = str(row['Vị trí'])
            kw = [k.strip() for k in str(row.get('Từ khóa mở rộng', 'du lịch, cảnh đẹp')).split(',') if k.strip()]
            img = str(row.get('Ảnh', ''))
            desc = str(row.get('Mô tả', ''))

    event = {
        "event_id": f"review_{int(time.time())}",
        "name": spot_name,
        "rating": float(rating),
        "user_name": user_name,
        "review_text": review_text,
        "latitude": lat,
        "longitude": lng,
        "location": loc,
        "keywords": kw,
        "image": img,
        "description": desc,
        "is_user_review": True,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

    print("="*75)
    print("  KỊCH BẢN DEMO: GỬI ĐÁNH GIÁ TRỰC TIẾP CỦA KHÁCH VÀO KAFKA (LIVE INGEST)")
    print("="*75)
    print(f"[*] Khách hàng:   {user_name}")
    print(f"[*] Địa điểm:     {spot_name} (📍 {loc} | Tọa độ: {lat}, {lng})")
    print(f"[*] Đánh giá:     ★ {rating}/5")
    print(f"[*] Lời nhận xét: \"{review_text}\"")
    print(f"[*] Thời gian:    {event['timestamp']}")
    print("-" * 75)
    print("[*] Đang kết nối tới Apache Kafka Broker cổng localhost:9092...")

    try:
        producer = KafkaProducer(
            bootstrap_servers='localhost:9092',
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
            api_version=(2, 8, 1)
        )
        producer.send('tourism-input', value=event)
        producer.flush()
        producer.close()
        print("\n[+] ĐÃ BẮN THÀNH CÔNG VÀO KAFKA TOPIC 'tourism-input'!")
        print(">>> 1. Hãy nhìn sang Terminal Spark Streaming để thấy Spark bắt được và gán cụm MLlib.")
        print(">>> 2. Hãy nhìn thư mục HDFS Parquet Lake để thấy tệp Parquet được ghi nối tiếp.")
        print(">>> 3. Hãy nhìn màn hình Web Dashboard (http://localhost:5000) để thấy thẻ đánh giá này xuất hiện ngay tức thì!")
        print("="*75)
    except Exception as e:
        print(f"[-] Lỗi kết nối Kafka: {e}")

if __name__ == '__main__':
    args = sys.argv[1:]
    spot = args[0] if len(args) > 0 else "Bà Nà Hills"
    rate = float(args[1]) if len(args) > 1 else 5.0
    text = args[2] if len(args) > 2 else "Trải nghiệm du lịch thực tế tuyệt vời, cảnh quan đẳng cấp!"
    user = args[3] if len(args) > 3 else "Nguyễn Trường Nhu (Báo cáo Big Data HUIT)"
    send_live_review(spot, rate, user, text)
