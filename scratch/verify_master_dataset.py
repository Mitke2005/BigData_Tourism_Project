"""
KIỂM ĐỊNH TÍNH TOÀN VẸN VÀ BẤT BIẾN CỦA MASTER DATASET
=============================================================================
Đồ án: Phân tích dữ liệu du lịch & gợi ý lịch trình thông minh (Lambda Architecture)
Mục tiêu:
1. So khớp từng dòng của dataset master (STT duy nhất, tọa độ trong Việt Nam,
   rating được chuẩn hóa đúng công thức từ 'Đánh giá ').
2. Kiểm tra mã băm SHA256 đối chiếu với data/MASTER_DATASET.sha256.
3. Báo lỗi và dừng (exit 1) nếu phát hiện bất kỳ dấu hiệu sửa đổi, ghi đè hoặc sai lệch nào.
=============================================================================
"""

import os
import sys
import re
import hashlib
import pandas as pd

if sys.platform.startswith('win'):
    try:
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
        sys.stderr.reconfigure(encoding='utf-8', errors='replace')
    except Exception:
        pass

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CSV_PATH = os.path.join(PROJECT_ROOT, "data", "tourism_dataset_clean.csv")
HASH_PATH = os.path.join(PROJECT_ROOT, "data", "MASTER_DATASET.sha256")

def parse_normalized_rating(val, dataset_mean=4.33):
    if pd.isna(val):
        return dataset_mean
    s = str(val).strip().replace(',', '.')
    m = re.search(r'([0-9]+\.?[0-9]*)', s)
    if m:
        try:
            num = float(m.group(1))
            if num > 5.0 and num <= 50.0:
                num = num / 10.0
            return round(max(1.0, min(5.0, num)), 2)
        except Exception:
            return dataset_mean
    return dataset_mean

def verify_master_dataset(strict_hash=True):
    print("=" * 75)
    print("  KIỂM ĐỊNH CHẤT LƯỢNG & TÍNH TOÀN VẸN MASTER DATASET")
    print(f"  Tệp kiểm tra: {CSV_PATH}")
    print("=" * 75)

    if not os.path.exists(CSV_PATH):
        raise FileNotFoundError(f"Không tìm thấy master dataset tại: {CSV_PATH}")

    # 1. Kiểm tra mã băm SHA256
    with open(CSV_PATH, 'rb') as f:
        file_bytes = f.read()
        current_hash = hashlib.sha256(file_bytes).hexdigest()

    if not os.path.exists(HASH_PATH):
        print(f"[!] Chưa có tệp hash. Đang tạo {HASH_PATH} với hash: {current_hash}")
        with open(HASH_PATH, 'w', encoding='utf-8') as f:
            f.write(current_hash + '\n')
        expected_hash = current_hash
    else:
        with open(HASH_PATH, 'r', encoding='utf-8') as f:
            expected_hash = f.read().strip()

    if strict_hash and current_hash != expected_hash:
        raise ValueError(
            f"VI PHẠM TÍNH BẤT BIẾN CỦA MASTER DATASET!\n"
            f"Hash hiện tại : {current_hash}\n"
            f"Hash chuẩn    : {expected_hash}"
        )
    print(f"[+] SHA256 khớp chuẩn: {current_hash}")

    # 2. Đọc và kiểm định dữ liệu
    df = pd.read_csv(CSV_PATH, encoding='utf-8')
    total_rows = len(df)
    print(f"[+] Tổng số dòng bản ghi: {total_rows}")

    if total_rows == 0:
        raise ValueError("Master dataset trống rỗng!")

    # Kiểm tra tính duy nhất của STT
    if 'STT' not in df.columns:
        raise ValueError("Thiếu cột bắt buộc 'STT' trong dataset!")

    stt_counts = df['STT'].value_counts()
    duplicates = stt_counts[stt_counts > 1]
    if not duplicates.empty:
        raise ValueError(f"Phát hiện STT bị trùng lặp: {duplicates.to_dict()}")
    print("[+] STT duy nhất 100% (không có trùng lặp).")

    # Kiểm tra tọa độ địa lý Việt Nam (bao gồm đất liền và hai quần đảo Hoàng Sa, Trường Sa)
    # Vĩ độ [8.0, 24.0], Kinh độ [102.0, 118.0]
    invalid_coords = []
    for idx, r in df.iterrows():
        stt = r['STT']
        lat = float(r['Latitude'])
        lng = float(r['Longitude'])
        if not (8.0 <= lat <= 24.0) or not (102.0 <= lng <= 118.0):
            invalid_coords.append((stt, lat, lng))

    if invalid_coords:
        raise ValueError(f"Phát hiện {len(invalid_coords)} điểm đến có tọa độ ngoài lãnh thổ Việt Nam: {invalid_coords[:5]}")
    print("[+] Tọa độ địa lý hợp lệ trong phạm vi lãnh thổ Việt Nam (Lat 8.0-24.0, Lng 102.0-118.0, bao gồm Hoàng Sa & Trường Sa).")

    # Kiểm tra tính chuẩn hóa của cột Rating
    valid_raw_ratings = df['Đánh giá '].apply(lambda v: parse_normalized_rating(v, None)).dropna()
    mean_val = round(valid_raw_ratings.mean(), 2) if not valid_raw_ratings.empty else 4.33

    rating_mismatches = []
    for idx, r in df.iterrows():
        stt = r['STT']
        cur_rating = float(r['Rating'])
        if not (1.0 <= cur_rating <= 5.0):
            rating_mismatches.append((stt, "Out of bounds [1.0, 5.0]", cur_rating))
            continue
        expected = parse_normalized_rating(r['Đánh giá '], mean_val)
        if abs(cur_rating - expected) > 1e-3:
            rating_mismatches.append((stt, f"Raw '{r['Đánh giá ']}' -> Expected {expected}", f"Current {cur_rating}"))

    if rating_mismatches:
        raise ValueError(f"Phát hiện {len(rating_mismatches)} dòng có Rating không khớp chuẩn hóa: {rating_mismatches[:5]}")
    print(f"[+] Cột Rating được chuẩn hóa khớp chính xác 100% với cột 'Đánh giá ' (trung bình: {mean_val}).")

    print("[V] KIỂM ĐỊNH HOÀN TẤT: Master dataset hoàn toàn hợp lệ, toàn vẹn và bất biến!")
    return True

if __name__ == '__main__':
    try:
        verify_master_dataset(strict_hash=True)
        sys.exit(0)
    except Exception as e:
        print(f"[-] LỖI XÁC MINH: {e}", file=sys.stderr)
        sys.exit(1)
