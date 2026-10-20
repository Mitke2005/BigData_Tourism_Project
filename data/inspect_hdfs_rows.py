import os
import sys
import glob
import pandas as pd
import pyarrow.parquet as pq

if sys.platform.startswith('win'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

lake_dir = "D:/BigData/DOAN/data/hdfs_lake/parquet"
files = glob.glob(os.path.join(lake_dir, "part_*.parquet"))

print("="*75)
print("  DANH SÁCH CÁC DÒNG (ROWS) THỰC TẾ ĐANG LƯU TRONG HDFS DATA LAKE")
print("="*75)
print(f"[*] Tổng số file phân mảnh part-files trên HDFS: {len(files)} files")

# 1. Các dòng Đánh giá của khách (Review Rows)
review_files = [f for f in files if "review" in os.path.basename(f)]
print(f"\n[1] CÁC DÒNG ĐÁNH GIÁ THỰC TẾ CỦA KHÁCH HÀNG ({len(review_files)} dòng):")
for rf in review_files[-5:]:
    try:
        df = pq.read_table(rf).to_pandas()
        row = df.iloc[0]
        print(f"  • File: {os.path.basename(rf)}")
        print(f"    - Tên khách:  {row.get('user_name')}")
        print(f"    - Địa điểm:   {row.get('name')} (📍 {row.get('location')})")
        print(f"    - Số sao:     ★ {row.get('rating')}/5")
        print(f"    - Nhận xét:   \"{row.get('review_text')}\"")
        print(f"    - Tọa độ:     Lat {row.get('latitude')}, Lon {row.get('longitude')}")
        print(f"    - Thời gian:  {row.get('timestamp')}")
        print("-" * 65)
    except Exception as e:
        pass

# 2. Các dòng sự kiện phân luồng (Stream Event Rows)
print(f"\n[2] CÁC DÒNG DỮ LIỆU ĐỊA ĐIỂM DU LỊCH ĐƯỢC PHÂN MẢNH TRÊN HDFS (Mẫu 6 dòng):")
sample_files = [f for f in files if "review" not in os.path.basename(f)][:6]
for sf in sample_files:
    try:
        df = pq.read_table(sf).to_pandas()
        row = df.iloc[0]
        print(f"  • File: {os.path.basename(sf):<22} | #{row.get('stt', 0):<3} {row.get('name'):<26} | 📍 {row.get('location'):<12} | ⭐ {row.get('rating')}")
    except Exception as e:
        pass

print("="*75)
