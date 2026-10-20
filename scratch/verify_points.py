import sys
if sys.platform.startswith('win'):
    try:
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    except:
        pass
import urllib.request
import json

base_url = "http://127.0.0.1:5000"

print("--- 1. KIỂM TRA HỆ THỐNG (/api/system_status) ---")
try:
    with urllib.request.urlopen(f"{base_url}/api/system_status") as resp:
        data = json.loads(resp.read().decode('utf-8'))
        print("Status code:", resp.status)
        print("Spark synced:", data.get('spark', {}).get('status'), "| Spots count:", data.get('spark', {}).get('spots_in_catalog'))
        print("Kafka:", data.get('kafka', {}).get('status'))
        print("HDFS:", data.get('hdfs', {}).get('status'))
except Exception as e:
    print("Error:", e)

print("\n--- 2. KIỂM TRA ĐIỂM 2: LỊCH TRÌNH THIẾU CACHE (KHÔNG TỰ TÍNH LOCAL CSV) ---")
# Tỉnh chưa có cache -> Phải trả 404 waiting_spark
try:
    urllib.request.urlopen(f"{base_url}/api/itinerary_multiday?province=TinhKhongTonTai&days=3")
except urllib.error.HTTPError as e:
    res = json.loads(e.read().decode('utf-8'))
    print("[+] Test 2a (Tỉnh lạ): Mã lỗi", e.code, "| status =", res.get('status'), "| message =", res.get('message'))

# Tỉnh hợp lệ trong Kafka Cache (ví dụ Khánh Hòa, Kiên Giang) -> Phải trả 200 từ Kafka
try:
    with urllib.request.urlopen(f"{base_url}/api/itinerary_multiday?province=Kh%C3%A1nh+H%C3%B2a&days=3") as resp:
        res = json.loads(resp.read().decode('utf-8'))
        print("[+] Test 2b (Khánh Hòa): Mã thành công", resp.status, "| source =", res.get('source'), "| Số ngày =", len(res.get('itinerary', [])))
except Exception as e:
    print("[-] Error 2b:", e)

print("\n--- 3. KIỂM TRA ĐIỂM 3: ƯU TIÊN STT TUYỆT ĐỐI, BÁO LỖI NẾU ĐỊA ĐIỂM KHÔNG TỒN TẠI (KHÔNG GÁN STT=1) ---")
# 3a. STT không tồn tại (9999) -> Phải trả 400 error
req_data_invalid = json.dumps({
    "stt": 9999,
    "spot_name": "Bãi Dài Cam Ranh", # Dù tên trùng Bãi Dài Cam Ranh nhưng STT sai -> Phải báo lỗi vì ưu tiên STT tuyệt đối
    "user_name": "Tester",
    "rating": 5,
    "review_text": "Thử nghiệm STT sai"
}).encode('utf-8')

req = urllib.request.Request(f"{base_url}/api/submit_review", data=req_data_invalid, headers={"Content-Type": "application/json"})
try:
    urllib.request.urlopen(req)
except urllib.error.HTTPError as e:
    res = json.loads(e.read().decode('utf-8'))
    print("[+] Test 3a (STT 9999 không tồn tại): Mã lỗi", e.code, "| status =", res.get('status'), "| message =", res.get('message'))

# 3b. STT hợp lệ (STT = 2 - Tháp Bà Ponagar)
req_data_valid = json.dumps({
    "stt": 2,
    "user_name": "Du khách Hà Nội",
    "rating": 5,
    "review_text": "Tháp Bà Ponagar rất cổ kính và linh thiêng!"
}).encode('utf-8')
req2 = urllib.request.Request(f"{base_url}/api/submit_review", data=req_data_valid, headers={"Content-Type": "application/json"})
try:
    with urllib.request.urlopen(req2) as resp:
        res = json.loads(resp.read().decode('utf-8'))
        print("[+] Test 3b (STT 2 hợp lệ): Mã thành công", resp.status, "| status =", res.get('status'), "| spot =", res.get('event', {}).get('name'), "| STT =", res.get('event', {}).get('stt'))
except Exception as e:
    print("[-] Error 3b:", e)
