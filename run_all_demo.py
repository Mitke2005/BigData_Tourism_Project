import os
import sys
import time
import json
import uuid
import socket
import signal
import subprocess
import webbrowser

if sys.platform.startswith('win'):
    try:
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
        sys.stderr.reconfigure(encoding='utf-8', errors='replace')
    except Exception:
        pass

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)

# Thiết lập môi trường Hadoop & winutils tự động trên Windows
HADOOP_HOME = os.environ.get("HADOOP_HOME", "C:\\hadoop" if sys.platform.startswith('win') else "/usr/local/hadoop")
os.environ['HADOOP_HOME'] = HADOOP_HOME
os.environ['hadoop.home.dir'] = HADOOP_HOME
JAVA_HOME = "C:\\jdk-17" if sys.platform.startswith('win') else os.environ.get("JAVA_HOME", "/usr/lib/jvm/java-17-openjdk-amd64")
os.environ['JAVA_HOME'] = JAVA_HOME
os.environ['HADOOP_USER_NAME'] = 'mitke'
if sys.platform.startswith('win'):
    bin_path = os.path.join(HADOOP_HOME, "bin")
    java_bin = os.path.join(JAVA_HOME, "bin")
    current_path = os.environ.get("PATH", "")
    if bin_path not in current_path:
        current_path = bin_path + os.pathsep + current_path
    if java_bin not in current_path:
        current_path = java_bin + os.pathsep + current_path
    os.environ["PATH"] = current_path

CHILD_PROCESSES = []


def register_process(proc):
    if proc:
        CHILD_PROCESSES.append(proc)
    return proc


def cleanup_all_processes():
    print("\n[*] Đang dọn dẹp và dừng các tiến trình con...")
    for proc in reversed(CHILD_PROCESSES):
        if proc and proc.poll() is None:
            try:
                proc.terminate()
                time.sleep(0.3)
                if proc.poll() is None:
                    proc.kill()
            except Exception:
                pass
    print("[+] Toàn bộ tiến trình đã được dừng an toàn.")


def signal_handler(sig, frame):
    cleanup_all_processes()
    sys.exit(0)


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


def check_port(host, port, timeout=1.5):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(timeout)
    try:
        s.connect((host, port))
        s.close()
        return True
    except Exception:
        return False


def ensure_kafka_running():
    if check_port('localhost', 9092):
        print("[+] Kafka Broker đang hoạt động trên cổng 9092.")
        return None
    # 1. Thử WSL nếu có
    try:
        r = subprocess.run(["wsl", "-e", "test", "-d", "/home/mitke/kafka"], capture_output=True, timeout=3)
        if r.returncode == 0:
            print("[*] Phát hiện Kafka trên WSL. Đang khởi động Kafka & Zookeeper trong WSL...")
            subprocess.run(["wsl", "-e", "bash", "-c", "/home/mitke/start_services.sh"], capture_output=True, timeout=15)
            for _ in range(15):
                if check_port('localhost', 9092):
                    print("[+] Kafka Broker đã sẵn sàng trên cổng 9092!")
                    return True
                time.sleep(1)
    except Exception:
        pass

    # 2. Thử đường dẫn Windows cục bộ
    bat_path = "D:\\BigData\\kafka\\bin\\windows\\kafka-server-start.bat"
    conf_path = "D:\\BigData\\kafka\\config\\server.properties"
    if os.path.exists(bat_path) and os.path.exists(conf_path):
        print("[*] Đang khởi động Kafka Broker trên Windows...")
        p = subprocess.Popen([bat_path, conf_path], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        register_process(p)
        time.sleep(4)
        return p
    else:
        print("[!] Cảnh báo: Kafka chưa mở trên cổng 9092.")
        return None


def ensure_hdfs_running():
    if check_port('localhost', 9000):
        print("[+] HDFS NameNode đang hoạt động trên cổng 9000.")
        return True
    print("[*] Chưa phát hiện HDFS NameNode trên cổng 9000. Đang tự động khởi động dịch vụ Hadoop HDFS...")
    try:
        from hadoop.hdfs_sync import ensure_hdfs_services
        return ensure_hdfs_services()
    except Exception as e:
        print(f"[!] Cảnh báo khởi động HDFS: {e}")
        return False


def run_ensure_topics():
    print("[*] [Bước 0] Tạo và xác thực 6 Kafka Topics chuẩn Lambda Architecture...")
    from messaging.create_topics import ensure_all_topics
    success = ensure_all_topics()
    if not success:
        print("[!] Không thể kết nối Kafka để kiểm tra topics. Vui lòng đảm bảo Kafka đang chạy.")


def sync_hdfs():
    print("[*] [Bước 1] Đưa Dataset lên hệ thống lưu trữ phân tán HDFS (bảo toàn SHA-256)...")
    hdfs_script = os.path.join(BASE_DIR, "hadoop", "hdfs_sync.py")
    res = subprocess.run([sys.executable, hdfs_script])
    if res.returncode != 0:
        print(f"[!] Cảnh báo: Đồng bộ HDFS trả về mã lỗi {res.returncode}.")


def run_spark_batch_train():
    print("[*] [Bước 2] Huấn luyện Batch Layer với PySpark MLlib và nạp Kafka State Store...")
    train_script = os.path.join(BASE_DIR, "spark", "spark_batch_train.py")
    res = subprocess.run([sys.executable, train_script], env=os.environ.copy())
    if res.returncode != 0:
        raise RuntimeError(f"Job batch huấn luyện 'spark_batch_train.py' thất bại với mã lỗi {res.returncode}. DỪNG PIPELINE NGAY LẬP TỨC!")


def wait_for_streaming_heartbeat(session_id: str, p_spark=None, timeout_sec=60):
    print(f"[*] Đang lắng nghe nhịp tim 'streaming' (session: {session_id}) trên topic 'tourism-control' (tối đa {timeout_sec}s)...")
    try:
        from kafka import KafkaConsumer  # type: ignore
    except ImportError:
        try:
            from kafka_python_ng import KafkaConsumer  # type: ignore
        except ImportError:
            time.sleep(5)
            return True

    consumer = None
    start_time = time.time()
    try:
        consumer = KafkaConsumer(
            "tourism-control",
            bootstrap_servers="localhost:9092",
            auto_offset_reset='latest',
            enable_auto_commit=False,
            consumer_timeout_ms=1000,
            api_version=(2, 8, 1)
        )
        while time.time() - start_time < timeout_sec:
            # 1. Kiểm tra nếu tiến trình Spark đã chết sớm
            if p_spark and p_spark.poll() is not None:
                raise RuntimeError(f"Tiến trình Spark Streaming đã thoát sớm với mã lỗi {p_spark.poll()}. DỪNG PIPELINE!")

            records = consumer.poll(timeout_ms=1000)
            for tp, msgs in records.items():
                for msg in msgs:
                    key_str = (msg.key.decode('utf-8', errors='ignore') if msg.key else "").strip()
                    if key_str in ("streaming", "streaming:heartbeat"):
                        try:
                            val = json.loads(msg.value.decode('utf-8'))
                            status = str(val.get("status", "")).upper()
                            val_sess = str(val.get("session_id", "")).strip()
                            ts_ms = float(val.get("ts_ms", 0))
                            age_ms = time.time() * 1000 - ts_ms
                            if status == "ACTIVE" and val_sess == session_id and age_ms < 30000:
                                print(f"[+] Nhận được heartbeat hợp lệ (ACTIVE, session_id={session_id}, độ mới {round(age_ms/1000, 1)}s) từ Spark Structured Streaming sau {round(time.time() - start_time, 2)}s!")
                                return True
                        except Exception:
                            pass
            elapsed = int(time.time() - start_time)
            if elapsed > 0 and elapsed % 10 == 0:
                print(f"[*] Đang chờ Spark Streaming heartbeat... ({elapsed}/{timeout_sec}s)")
        raise RuntimeError(f"Quá {timeout_sec}s không nhận được heartbeat hợp lệ (ACTIVE, đúng session_id '{session_id}') từ Spark Structured Streaming. DỪNG PIPELINE NGAY LẬP TỨC!")
    finally:
        if consumer:
            try:
                consumer.close()
            except Exception:
                pass


def run_full_pipeline():
    print("=" * 80)
    print("  KHỞI CHẠY KIẾN TRÚC BIG DATA DU LỊCH (KAFKA-FIRST LAMBDA ARCHITECTURE):")
    print("  1. Check Network Ports (Kafka 9092, HDFS 9000)")
    print("  2. Ensure Kafka Topics (compacted state & stream topics)")
    print("  3. Ensure Master Dataset on HDFS")
    print("  4. PySpark MLlib Batch Training -> Kafka State Store")
    print("  5. Spark Structured Streaming (Kafka readStream -> Micro-Batch -> Parquet HDFS & Kafka)")
    print("  6. Chờ Heartbeat -> Web Dashboard (In-Memory Materialized View)")
    print("  7. Kafka Event Producer (SIMULATED_VIEW)")
    print("=" * 80)

    try:
        # 1. Kiểm tra Kafka & HDFS
        ensure_kafka_running()
        ensure_hdfs_running()

        # 2. Tạo topics nếu chưa có
        run_ensure_topics()

        # 3. Nạp Master Dataset lên HDFS
        sync_hdfs()

        # 4. Chạy Batch Training (Dừng ngay nếu lỗi)
        run_spark_batch_train()

        # 5. Khởi chạy Spark Structured Streaming trong nền với session_id nhất quán
        session_id = f"stream_{uuid.uuid4().hex[:12]}_{int(time.time())}"
        spark_env = os.environ.copy()
        spark_env["SPARK_STREAMING_SESSION_ID"] = session_id

        print(f"[*] [Bước 3] Khởi chạy Spark Structured Streaming (Phiên: {session_id})...")
        spark_script = os.path.join(BASE_DIR, "spark", "spark_structured_streaming.py")
        p_spark = subprocess.Popen([sys.executable, spark_script], env=spark_env)
        register_process(p_spark)

        # 5b. Khởi chạy HDFS Stream Archiver (Tiến trình lưu trữ HDFS riêng, không block đường phản hồi)
        print("[*] Khởi chạy HDFS Stream Archiver (tiến trình lưu trữ Parquet HDFS bất đồng bộ)...")
        archiver_script = os.path.join(BASE_DIR, "spark", "hdfs_stream_archiver.py")
        p_archiver = subprocess.Popen([sys.executable, "-u", archiver_script], env=os.environ.copy())
        register_process(p_archiver)

        # 6. Chờ heartbeat từ streaming với kiểm tra session_id và tiến trình sống
        wait_for_streaming_heartbeat(session_id=session_id, p_spark=p_spark, timeout_sec=60)

        # 7. Khởi chạy Web Dashboard
        print("[*] [Bước 4] Khởi chạy Web Dashboard (http://localhost:5000)...")
        web_script = os.path.join(BASE_DIR, "web_dashboard", "app.py")
        p_web = subprocess.Popen([sys.executable, web_script])
        register_process(p_web)

        # Mở trình duyệt sau 3s
        def open_browser():
            time.sleep(3)
            print("[+] Mở trình duyệt tới: http://localhost:5000")
            webbrowser.open("http://localhost:5000")
        import threading
        threading.Thread(target=open_browser, daemon=True).start()

        # 8. Khởi chạy Kafka Producer phát sự kiện mô phỏng
        print("[*] [Bước 5] Khởi chạy Kafka Producer phát sự kiện mô phỏng...")
        prod_script = os.path.join(BASE_DIR, "messaging", "producer.py")
        p_prod = subprocess.Popen([sys.executable, prod_script, "--no-wait-streaming"])
        register_process(p_prod)

        print("[+] Toàn bộ hệ thống đang hoạt động! Nhấn Ctrl+C để dừng toàn bộ.")
        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        print("\n[!] Nhận tín hiệu dừng từ người dùng.")
    finally:
        cleanup_all_processes()


def run_spark_sql():
    print("[*] Đang thực thi phân tích dữ liệu phân tán với Spark SQL...")
    sql_script = os.path.join(BASE_DIR, "spark", "spark_sql_analytics.py")
    subprocess.run([sys.executable, sql_script])


def run_airflow_validation():
    print("[*] Đang thực thi kiểm định Airflow Pipeline...")
    dag_script = os.path.join(BASE_DIR, "airflow", "tourism_pipeline_dag.py")
    subprocess.run([sys.executable, dag_script])


def run_benchmark():
    print("[*] Đang thực thi đánh giá mô hình MLlib & Benchmark định dạng lưu trữ...")
    bm_script = os.path.join(BASE_DIR, "spark", "benchmark_and_evaluation.py")
    subprocess.run([sys.executable, bm_script])


def run_batch_sync_pipeline():
    print("[*] Đang thực thi chu kỳ đồng bộ đánh giá vào HDFS & Parquet Lake (Lambda Closed-Loop)...")
    sync_script = os.path.join(BASE_DIR, "hadoop", "batch_sync_pipeline.py")
    subprocess.run([sys.executable, sync_script])


def main():
    print("=" * 80)
    print("  ĐỒ ÁN BIG DATA: PHÂN TÍCH DỮ LIỆU DU LỊCH & GỢI Ý LỊCH TRÌNH THÔNG MINH")
    print("  KIẾN TRÚC LAMBDA KAFKA-FIRST: HDFS -> SPARK -> KAFKA -> RAM VIEW -> WEB")
    print("=" * 80)
    print("  [1] Khởi chạy toàn trình Pipeline Live Demo")
    print("  [2] Chạy phân tích truy vấn dữ liệu lớn Spark SQL (Chương 3)")
    print("  [3] Chạy kiểm định Airflow DAG Pipeline")
    print("  [4] Chỉ chạy bước nạp dữ liệu lên HDFS")
    print("  [5] Chạy đánh giá mô hình MLlib & Benchmark định dạng lưu trữ")
    print("  [6] Đồng bộ đánh giá tích lũy vào HDFS & Data Lake Parquet (Lambda Batch Sync)")
    print("=" * 80)

    if len(sys.argv) > 1:
        choice = sys.argv[1].replace('-', '').strip()
    else:
        try:
            choice = input("Lựa chọn chế độ khởi chạy [Mặc định 1]: ").strip()
        except (EOFError, KeyboardInterrupt):
            choice = '1'
    if choice == '2':
        run_spark_sql()
    elif choice == '3':
        run_airflow_validation()
    elif choice == '4':
        sync_hdfs()
    elif choice == '5':
        run_benchmark()
    elif choice == '6':
        run_batch_sync_pipeline()
    else:
        run_full_pipeline()


if __name__ == '__main__':
    main()

