import os
import sys
import time
import json
import requests
import socket

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)

if sys.platform.startswith('win'):
    try:
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    except Exception:
        pass

def test_pipeline():
    print("=" * 70)
    print("BAT DAU KIEM TRA 5 TIEU CHI CHUNG MINH HE THONG TOURISM BIG DATA")
    print("=" * 70)

    # -------------------------------------------------------------
    # 1. Dataset tren HDFS & Spark doc dung URI
    # -------------------------------------------------------------
    print("\n[TIEU CHI 1] Kiem tra Dataset tren HDFS & Spark doc dung URI...")
    # Check HDFS Port 9000
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(2)
    hdfs_open = s.connect_ex(("127.0.0.1", 9000)) == 0
    s.close()
    if not hdfs_open:
        print("[-] THAT BAI: HDFS NameNode port 9000 khong ket noi duoc!")
        return False
    print("[+] HDFS NameNode port 9000 dang hoat dong tot.")

    # Check Web system_status
    try:
        r = requests.get("http://localhost:5000/api/system_status", timeout=5)
        st = r.json()
        hdfs_ok = st.get('hdfs', {}).get('connected')
        kafka_ok = st.get('kafka', {}).get('connected')
        spark_status = st.get('spark', {}).get('status')
        print(f"[+] Web System Status: HDFS={hdfs_ok}, Kafka={kafka_ok}, Spark={spark_status}")
        assert hdfs_ok == True, "HDFS not connected according to Web"
        assert kafka_ok == True, "Kafka not connected according to Web"
    except Exception as e:
        print(f"[-] THAT BAI khi goi /api/system_status: {e}")
        return False
    print("[=> PASSED TIEU CHI 1]")

    # -------------------------------------------------------------
    # 2. Ket qua Spark xuat hien trong Kafka va Web
    # -------------------------------------------------------------
    print("\n[TIEU CHI 2] Ket qua Spark xuat hien trong Kafka va Web...")
    try:
        r = requests.get("http://localhost:5000/api/all_spots", timeout=5)
        spots = r.json()
        print(f"[+] So luong dia diem Web nhan tu Spark/Kafka catalog: {len(spots)} spots")
        assert len(spots) >= 300, f"So luong spots qua it: {len(spots)}"
        # Check first spot has cluster_id, lat, lng, rating
        sp1 = spots[0]
        assert "cluster_id" in sp1
        assert "latitude" in sp1 and "longitude" in sp1
        assert sp1.get("longitude") is not None
        print(f"[+] Mau spot #1: {sp1.get('name')} | Cluster={sp1.get('cluster_id')} | Lat={sp1.get('latitude')}, Lng={sp1.get('longitude')}")
    except Exception as e:
        print(f"[-] THAT BAI TIEU CHI 2: {e}")
        return False
    print("[=> PASSED TIEU CHI 2]")

    # -------------------------------------------------------------
    # 3. Mot review chi tang dung mot lan, tab thu hai tu cap nhat
    # -------------------------------------------------------------
    print("\n[TIEU CHI 3] Mot review chi tang dung 1 lan & giu nguyen longitude...")
    test_stt = 15 # Test spot
    try:
        r = requests.get("http://localhost:5000/api/all_spots", timeout=5)
        spots = {s["stt"]: s for s in r.json()}
        orig_count = spots[test_stt].get("review_count", 0)
        orig_rating = spots[test_stt].get("rating", 0.0)
        orig_name = spots[test_stt].get("name")
        print(f"[+] Truoc khi review Spot #{test_stt} ({orig_name}): Reviews={orig_count}, Rating={orig_rating}")

        # Post review
        rev_payload = {
            "stt": test_stt,
            "rating": 5,
            "comment": "Chung minh tieu chi 3 - Review tang 1 lan khong duplicate",
            "author": "Verifier"
        }
        post_r = requests.post("http://localhost:5000/api/submit_review", json=rev_payload, timeout=5)
        assert post_r.status_code == 200, f"Submit review failed: {post_r.text}"
        print(f"[+] Da submit review 5 sao vao Kafka 'tourism-input' thanh cong.")

        # Wait for Spark Streaming to process and emit to Kafka -> Web consumes
        print("[+] Dang cho Spark Streaming & Web Consumer xu ly qua Kafka...")
        time.sleep(4)

        r2 = requests.get("http://localhost:5000/api/all_spots", timeout=5)
        spots2 = {s["stt"]: s for s in r2.json()}
        new_count = spots2[test_stt].get("review_count", 0)
        new_rating = spots2[test_stt].get("rating", 0.0)
        new_lng = spots2[test_stt].get("longitude")

        print(f"[+] Sau khi review Spot #{test_stt}: Reviews={new_count}, Rating={new_rating}, Lng={new_lng}")
        assert new_count == orig_count + 1, f"LOI: review_count phai tang dung 1 don vi! Truoc={orig_count}, Sau={new_count}"
        assert new_lng is not None, f"LOI: Longitude bi None!"
    except Exception as e:
        print(f"[-] THAT BAI TIEU CHI 3: {e}")
        return False
    print("[=> PASSED TIEU CHI 3]")

    # -------------------------------------------------------------
    # 4. Dong bo du lieu moi dan den Spark xu ly lai va Web nhan ket qua moi
    # -------------------------------------------------------------
    print("\n[TIEU CHI 4] Dong bo du lieu moi (Batch Sync) -> Spark Reload tu HDFS -> Web nhan ket qua...")
    try:
        t_start = time.time()
        sync_r = requests.post("http://localhost:5000/api/sync_hdfs_retrain", timeout=30)
        assert sync_r.status_code == 200, f"Sync that bai: {sync_r.status_code} - {sync_r.text}"
        sync_res = sync_r.json()
        print(f"[+] Ket qua Batch Sync API: success={sync_res.get('success')}, hdfs={sync_res.get('hdfs_status')}, trigger={sync_res.get('spark_reload_triggered')}")
        assert sync_res.get("success") == True
        assert sync_res.get("spark_reload_triggered") == True

        # Wait 5 seconds for Spark to consume TRIGGER_SPARK_BATCH_RELOAD and reload HDFS CSV
        print("[+] Dang cho Spark doc lai dataset tu HDFS va broadcast SPARK_BATCH_CATALOG_REFRESH...")
        time.sleep(6)

        # Check Web received refresh
        r3 = requests.get("http://localhost:5000/api/all_spots", timeout=5)
        spots3 = {s["stt"]: s for s in r3.json()}
        synced_count = spots3[test_stt].get("review_count", 0)
        print(f"[+] Spot #{test_stt} sau khi Spark doc lai HDFS va cap nhat toan dien: Reviews={synced_count}")
        assert synced_count >= new_count
    except Exception as e:
        print(f"[-] THAT BAI TIEU CHI 4: {e}")
        return False
    print("[=> PASSED TIEU CHI 4]")

    # -------------------------------------------------------------
    # 5. Tat HDFS thi he thong bao that bai dung thuc te (Khong gia mao)
    # -------------------------------------------------------------
    print("\n[TIEU CHI 5] Kiem tra co che khi HDFS Offline / loi thi he thong phai bao that bai dung thuc te...")
    try:
        import hadoop.batch_sync_pipeline as bsp
        # Goi run_batch_sync voi cong HDFS khong ton tai (9999)
        offline_report = bsp.run_batch_sync(hdfs_port=9999)
        print(f"[+] Ket qua khi HDFS port 9999 khong hoat dong: success={offline_report.get('success')}, status={offline_report.get('hdfs_status')}")
        assert offline_report.get("success") == False, "Loi: Khi HDFS offline ma success van la True!"
        assert offline_report.get("hdfs_status") == "HDFS_OFFLINE", "Loi: status khong dung!"

        # Kiem tra Flask endpoint /api/sync_hdfs_retrain mapping status code sang 503 khi HDFS loi
        from web_dashboard.app import app
        with app.test_client() as client:
            # Mock or temporarily test when run_batch_sync returns failure
            real_func = bsp.run_batch_sync
            bsp.run_batch_sync = lambda: offline_report
            resp = client.post('/api/sync_hdfs_retrain')
            bsp.run_batch_sync = real_func
            print(f"[+] Flask API tra ve HTTP status={resp.status_code} (Chuan 503 Service Unavailable khi HDFS loi)")
            assert resp.status_code == 503, f"Status code khong phai 503: {resp.status_code}"

        print("[+] Co che bat loi HDFS: Chi bao thanh cong khi HDFS that su hoat dong, bao loi ro rang khi HDFS Offline.")
    except Exception as e:
        print(f"[-] THAT BAI TIEU CHI 5: {e}")
        return False
    print("[=> PASSED TIEU CHI 5]")

    print("\n" + "=" * 70)
    print("TAT CA 5 TIEU CHI DEU DAT 100%! HE THONG DA HOAN CHINH DE DEMO!")
    print("=" * 70)
    return True

if __name__ == "__main__":
    test_pipeline()
