import pandas as pd
import numpy as np
import re
import os

PROVINCE_COORDS = {
    'Nghệ An': (18.67, 105.68), 'Thanh Hóa': (19.80, 105.77), 'Bến Tre': (10.24, 106.37),
    'Lai Châu': (22.39, 103.46), 'Quảng Trị': (16.82, 107.10), 'Thái Bình': (20.44, 106.33),
    'Bình Phước': (11.75, 106.90), 'Vĩnh Phúc': (21.31, 105.60), 'Hậu Giang': (9.78, 105.47),
    'Khánh Hòa': (12.24, 109.19), 'Lào Cai': (22.48, 103.97), 'Quảng Bình': (17.47, 106.60),
    'Thái Nguyên': (21.59, 105.84), 'Bắc Giang': (21.27, 106.20), 'Bắc Kạn': (22.14, 105.83),
    'Trà Vinh': (9.93, 106.34), 'Tuyên Quang': (21.82, 105.21), 'Yên Bái': (21.72, 104.91),
    'Đắk Nông': (12.00, 107.69), 'Hà Giang': (22.82, 104.98), 'Hải Dương': (20.94, 106.33),
    'Hưng Yên': (20.65, 106.05), 'Kiên Giang': (10.01, 105.08), 'Long An': (10.53, 106.41),
    'Ninh Bình': (20.25, 105.97), 'Ninh Thuận': (11.56, 108.98), 'Phú Thọ': (21.32, 105.22),
    'Phú Yên': (13.09, 109.31), 'Quảng Nam': (15.57, 108.47), 'Quảng Ngãi': (15.12, 108.80),
    'Quảng Ninh': (20.95, 107.08), 'Sóc Trăng': (9.60, 105.97), 'Sơn La': (21.32, 103.91),
    'Tây Ninh': (11.31, 106.10), 'Bạc Liêu': (9.29, 105.72), 'Bắc Ninh': (21.18, 106.07),
    'Bình Thuận': (10.93, 108.10), 'Cao Bằng': (22.66, 106.26), 'Đắk Lắk': (12.67, 108.04),
    'Đồng Tháp': (10.46, 105.63), 'Gia Lai': (13.98, 108.00), 'Hà Nam': (20.54, 105.92),
    'Hà Tĩnh': (18.34, 105.90), 'Hải Phòng': (20.84, 106.68), 'Hòa Bình': (20.81, 105.34),
    'Hà Nội': (21.0285, 105.8542), 'Hồ Chí Minh': (10.8231, 106.6297), 'Đà Nẵng': (16.0544, 108.2022),
    'Cần Thơ': (10.0452, 105.7469), 'Lâm Đồng': (11.9404, 108.4583), 'Thừa Thiên Huế': (16.4637, 107.5909),
    'Bà Rịa - Vũng Tàu': (10.4114, 107.1362), 'Bình Định': (13.7820, 109.2197), 'An Giang': (10.3759, 105.4185)
}

def clean_rating(val):
    if pd.isna(val):
        return 4.5
    s = str(val).strip()
    m_slash = re.match(r'^(\d{2})/5', s)
    if m_slash:
        return float(m_slash.group(1)) / 10.0
    match = re.search(r'([0-9]+\.?[0-9]*)', s)
    if match:
        try:
            r = float(match.group(1))
            if r > 5.0 and r <= 50.0:
                r = r / 10.0
            return max(1.0, min(5.0, r))
        except:
            return 4.5
    return 4.5

def clean_dataset():
    src_path = 'd:/BigData/DOAN/data/tourism_dataset.csv'
    out_path = 'd:/BigData/DOAN/data/tourism_dataset_clean.csv'
    df = pd.read_csv(src_path)

    # Standardize ratings
    df['Rating'] = df['Đánh giá '].apply(clean_rating)

    # Fill coordinates
    np.random.seed(42)
    for idx, row in df.iterrows():
        if pd.isna(row['Latitude']) or pd.isna(row['Longitude']):
            prov = str(row['Vị trí']).strip()
            if prov in PROVINCE_COORDS:
                base_lat, base_lng = PROVINCE_COORDS[prov]
            else:
                base_lat, base_lng = (16.0, 107.5) # Center of Vietnam
            # Add small jitter +/- 0.015 (~1.5km) so points don't stack directly on top
            jitter_lat = np.random.uniform(-0.015, 0.015)
            jitter_lng = np.random.uniform(-0.015, 0.015)
            df.at[idx, 'Latitude'] = round(base_lat + jitter_lat, 6)
            df.at[idx, 'Longitude'] = round(base_lng + jitter_lng, 6)

    # Fill expanded keywords
    for idx, row in df.iterrows():
        val = row['Từ Khóa (Mở rộng)']
        if pd.isna(val) or not str(val).strip():
            raw_kw = str(row['Từ Khóa']) if pd.notna(row['Từ Khóa']) else ''
            desc = str(row['Mô tả']) if pd.notna(row['Mô tả']) else ''
            tags = ['du lịch', 'tham quan', 'trải nghiệm']
            if 'biển' in desc.lower() or 'biển' in raw_kw.lower():
                tags.extend(['biển', 'tắm biển', 'nghĩ dưỡng', 'hóng gió'])
            if 'chùa' in desc.lower() or 'di tích' in desc.lower() or 'lịch sử' in desc.lower():
                tags.extend(['văn hóa', 'di tích', 'lịch sử', 'tâm linh'])
            if 'núi' in desc.lower() or 'rừng' in desc.lower() or 'thác' in desc.lower():
                tags.extend(['thiên nhiên', 'khám phá', 'trekking', 'ngoài trời'])
            tags.extend(['đi cùng gia đình', 'chụp ảnh', 'check-in'])
            df.at[idx, 'Từ Khóa (Mở rộng)'] = ', '.join([f'"{t}"' for t in set(tags)])

    # Fill raw keywords if null
    for idx, row in df.iterrows():
        if pd.isna(row['Từ Khóa']) or not str(row['Từ Khóa']).strip():
            df.at[idx, 'Từ Khóa'] = row['Từ Khóa (Mở rộng)']

    df.to_csv(out_path, index=False, encoding='utf-8')
    print('Cleaned dataset saved successfully!')
    print('Total rows:', len(df))
    print('Null Latitude:', df['Latitude'].isnull().sum())
    print('Null Longitude:', df['Longitude'].isnull().sum())
    print('Null Keywords:', df['Từ Khóa (Mở rộng)'].isnull().sum())

if __name__ == '__main__':
    clean_dataset()
