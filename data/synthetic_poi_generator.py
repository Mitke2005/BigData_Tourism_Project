"""
BỘ SINH DỮ LIỆU ĐỊA ĐIỂM GIẢ LẬP ĐỂ KIỂM THỬ TẢI HỆ THỐNG (STRESS TEST)
=============================================================================
CẢNH BÁO QUAN TRỌNG:
Tệp dữ liệu này được SINH NGẪU NHIÊN (synthetic data) hoàn toàn bằng thuật toán
nhằm mục đích kiểm thử tải, đo thông lượng và thử nghiệm khả năng mở rộng (Scalability)
của cụm HDFS / YARN / Spark với quy mô lớn.
Dữ liệu này KHÔNG PHẢI được trích xuất từ OpenStreetMap hay cơ sở dữ liệu thực địa.
=============================================================================
"""

import os
import sys
import numpy as np
import pandas as pd

if sys.platform.startswith('win'):
    try:
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
        sys.stderr.reconfigure(encoding='utf-8', errors='replace')
    except Exception:
        pass

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_OUTPUT_PATH = os.path.join(BASE_DIR, "data", "synthetic_poi_stress_test.csv")

def generate_synthetic_poi_dataset(num_records=5000, output_path=None):
    if output_path is None:
        output_path = DEFAULT_OUTPUT_PATH
        
    print("=" * 75)
    print(f"  SINH DỮ LIỆU KIỂM THỬ TẢI (STRESS TEST): {num_records} BẢN GHI")
    print("  (Dữ liệu sinh ngẫu nhiên, không trích xuất từ OpenStreetMap)")
    print("=" * 75)
    
    np.random.seed(42)
    categories = [
        ("Khu du lịch sinh thái", ["thiên nhiên", "ngoài trời", "nghỉ dưỡng", "chụp ảnh"]),
        ("Di tích lịch sử văn hóa", ["lịch sử", "di tích", "văn hóa", "tham quan", "kiến trúc"]),
        ("Bãi biển & Vịnh đảo", ["biển", "tắm biển", "hải sản", "nghĩ dưỡng", "hóng gió"]),
        ("Chùa & Đền thờ cổ", ["tâm linh", "chùa", "cầu an", "văn hóa", "thanh tịnh"]),
        ("Khu cắm trại & Trekking", ["núi", "trekking", "cắm trại", "thể lực tốt", "khám phá"]),
        ("Chợ đêm & Ẩm thực", ["chợ", "ẩm thực", "mua sắm", "đặc sản", "đi cùng gia đình"])
    ]
    
    provinces = [
        ("Hà Nội", 21.0285, 105.8542), ("Hồ Chí Minh", 10.8231, 106.6297),
        ("Đà Nẵng", 16.0544, 108.2022), ("Khánh Hòa", 12.2451, 109.1943),
        ("Lâm Đồng", 11.9404, 108.4583), ("Quảng Ninh", 20.9502, 107.0734),
        ("Kiên Giang", 10.0125, 105.0809), ("Thừa Thiên Huế", 16.4637, 107.5909),
        ("Ninh Bình", 20.2506, 105.9745), ("Bình Thuận", 10.9333, 108.1000)
    ]
    
    records = []
    for i in range(1, num_records + 1):
        prov, base_lat, base_lng = provinces[np.random.choice(len(provinces))]
        cat_name, tags = categories[np.random.choice(len(categories))]
        
        # Thêm độ lệch ngẫu nhiên trong phạm vi tỉnh
        lat = round(base_lat + np.random.uniform(-0.15, 0.15), 6)
        lng = round(base_lng + np.random.uniform(-0.15, 0.15), 6)
        rating = round(np.random.uniform(3.5, 5.0), 1)
        
        records.append({
            "synthetic_id": f"SYN_VN_{i:06d}",
            "name": f"{cat_name} {prov} #{i}",
            "location": prov,
            "category": cat_name,
            "rating": rating,
            "latitude": lat,
            "longitude": lng,
            "keywords": ", ".join([f'"{t}"' for t in tags])
        })
        
    df = pd.DataFrame(records)
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False, encoding='utf-8')
    print(f"[+] Đã tạo thành công {num_records} điểm dữ liệu giả lập tại: {output_path}")
    print(f"[*] Dung lượng tệp: {round(os.path.getsize(output_path)/1024, 2)} KB")
    return output_path

if __name__ == '__main__':
    generate_synthetic_poi_dataset(num_records=5000)
