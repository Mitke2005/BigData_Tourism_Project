# BẢNG KIỂM TRA BẰNG CHỨNG THỰC NGHIỆM (EVIDENCE CHECKLIST)
## Đồ Án Big Data: Phân Tích Dữ Liệu Du Lịch & Gợi Ý Lịch Trình Thông Minh (Kafka-First Lambda Architecture)

Tài liệu này cung cấp đầy đủ danh mục câu lệnh CLI, vị trí giao diện đồ họa (UI) và file log cần thu thập để làm bằng chứng thực nghiệm cho báo cáo đồ án và buổi bảo vệ trực tiếp trước hội đồng theo chuẩn **Kafka-First Lambda Architecture (100% không dùng SQLite)**.

---

### PHẦN 1. BẰNG CHỨNG HẠ TẦNG PHÂN TÁN (INFRASTRUCTURE)

#### 1.1 Apache Kafka (Topics, Partitions & Compacted State Topics)
* **Mục tiêu chứng minh:** 6 topics Kafka được cấu hình đúng: 3 topic phân vùng stream (`tourism-input`, `tourism-output`, `tourism-reviews` với 3 partitions) và 3 compacted state topics (`tourism-state-catalog`, `tourism-state-kmeans`, `tourism-state-itinerary` với `cleanup.policy=compact`).
* **Câu lệnh kiểm tra:**
  ```bash
  # Liệt kê và mô tả chi tiết các topic
  kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic tourism-input,tourism-output,tourism-reviews,tourism-state-catalog,tourism-state-kmeans,tourism-state-itinerary
  ```
* **Bằng chứng cần chụp/lưu:**
  - Ảnh chụp màn hình console hiển thị: `Topic: tourism-input  PartitionCount: 3  ReplicationFactor: 1`.
  - Cấu hình compacted state: `cleanup.policy=compact` trên `tourism-state-catalog`.

#### 1.2 Hadoop HDFS Distributed File System
* **Mục tiêu chứng minh:** Cấu trúc phân vùng dữ liệu HDFS chuẩn Data Lake (Raw Zone, Processed Zone, Streaming Zone, Analytics Zone, Dead-Letter Queue).
* **Câu lệnh kiểm tra:**
  ```bash
  # Kiểm tra tính bất biến của Master Dataset (Khóa SHA-256)
  python scratch/test_master_immutable.py

  # Liệt kê phân vùng Streaming Zone (micro-batches Parquet)
  hdfs dfs -ls -R /tourism_data/streaming_zone/events
  hdfs dfs -ls -R /tourism_data/streaming_zone/reviews

  # Liệt kê các bảng Parquet sau huấn luyện Batch
  hdfs dfs -ls -R /tourism_data/analytics_zone

  # Liệt kê thư mục Dead-Letter Queue
  hdfs dfs -ls -R /tourism_data/dead_letter

  # Kiểm tra báo cáo tình trạng cụm NameNode
  hdfs dfsadmin -report
  ```
* **Bằng chứng cần chụp/lưu:**
  - Web UI Hadoop NameNode tại: `http://localhost:9870` (tab **Overview** và **Datanodes** hiển thị 100% Online).
  - Terminal log kết quả `hdfs dfs -ls` hiển thị các thư mục `run_id=.../batch_id=...` và file `.parquet`.

---

### PHẦN 2. BẰNG CHỨNG BATCH LAYER & KAFKA STATE STORE

#### 2.1 Huấn Luyện Batch View (`spark/spark_batch_train.py`)
* **Mục tiêu chứng minh:** Spark MLlib đọc dataset bất biến từ HDFS, huấn luyện K-Means (k=5), TF-IDF Cosine similarity, FP-Growth, tính toán 55 lịch trình đa ngày và nạp toàn bộ vào Kafka State Store + HDFS Analytics Zone.
* **Câu lệnh thực thi:**
  ```bash
  python spark/spark_batch_train.py
  ```
* **Bằng chứng cần chụp/lưu:**
  - Log console hiển thị:
    - `[+] Đã huấn luyện xong K-Means (k=5) và lưu mô hình lên HDFS`
    - `[+] Đã xuất Parquet phân tán lên HDFS /tourism_data/analytics_zone/`
    - `[+] Đã publish 55 spots vào topic 'tourism-state-catalog'`
    - `[+] Đã publish mô hình K-Means vào topic 'tourism-state-kmeans'`
    - `[+] Đã publish lịch trình vào topic 'tourism-state-itinerary'`
    - `[+] Đã publish commit event vào topic 'tourism-control'`

#### 2.2 Spark Compaction (`spark/compact_streaming_zone.py`)
* **Mục tiêu chứng minh:** Gộp các file nhỏ Parquet vi mô từ `streaming_zone/events` và `streaming_zone/reviews` thành file lớn trong analytics zone, khử trùng lặp theo `event_id` với xác thực toàn vẹn 100%.
* **Câu lệnh thực thi:**
  ```bash
  python spark/compact_streaming_zone.py --older-than-minutes 10 --target all
  ```
* **Bằng chứng cần chụp/lưu:**
  - Log console hiển thị: `Verify Run: Đã ghi N bản ghi (kỳ vọng: N) — Hoàn thành compaction thành công`.

---

### PHẦN 3. BẰNG CHỨNG SPEED LAYER (SPARK STRUCTURED STREAMING)

#### 3.1 Spark Structured Streaming (`spark/spark_structured_streaming.py`)
* **Mục tiêu chứng minh:** Spark Structured Streaming hoạt động liên tục với micro-batch trigger 5s, đọc `tourism-input`, kiểm định schema, làm giàu cụm K-Means, ghi Parquet phân tán HDFS và publish đồng thời sang `tourism-reviews` và `tourism-output`.
* **Địa chỉ kiểm tra:**
  - Spark Web UI: `http://localhost:4040` (Tab: **Structured Streaming**).
* **Bằng chứng cần chụp/lưu:**
  - Biểu đồ **Input Rate vs Process Rate** khi có luồng tải từ producer.
  - Heartbeat chu kỳ 5 giây được publish đều đặn vào `tourism-control` (`key: streaming`).

---

### PHẦN 4. BẰNG CHỨNG CÁC KỊCH BẢN KIỂM THỬ TỰ ĐỘNG

Chạy toàn bộ bộ test kiểm chứng tự động:
```bash
python scratch/test_master_immutable.py
python scratch/test_idempotent_replay.py
python scratch/test_restart_recovery.py
python scratch/test_dead_letter.py
python scratch/test_sync_batch.py
python scratch/test_kafka_first_e2e.py
```

* **Ý nghĩa từng bài test:**
  1. `test_master_immutable.py`: Xác nhận Master Dataset không bao giờ bị ghi đè, hash SHA-256 bảo toàn `d646ecc6...`.
  2. `test_idempotent_replay.py`: Gửi cùng 1 `event_id` nhiều lần chỉ tính 1 lần, điểm số và số lượt đánh giá không bị nhân đôi.
  3. `test_restart_recovery.py`: Mô phỏng Web App tắt đột ngột và bật lại, khôi phục 100% Serving Layer từ Kafka State Streams mà không cần DB.
  4. `test_dead_letter.py`: Lọc chính xác các bản ghi JSON lỗi (thiếu ID, sai STT, rating ngoài [1,5], văn bản tràn) vào Dead-Letter Queue.
  5. `test_sync_batch.py`: Kiểm chứng chu kỳ Lambda Closed-Loop: Sau khi batch chạy, `synced_event_ids` được loại trừ chính xác, lượt đánh giá không bao giờ bị reset về 0.
  6. `test_kafka_first_e2e.py`: Kiểm tra toàn trình luồng phục vụ Lambda Merged View trong RAM.

---

### PHẦN 5. BẰNG CHỨNG ĐIỀU PHỐI AIRFLOW (ORCHESTRATION)

#### 5.1 Airflow DAG: `vietnam_tourism_bigdata_pipeline`
* **Mục tiêu chứng minh:** Quy trình tự động hóa 4 bước khép kín:
  `ensure_topics >> upload_master >> run_batch_job >> verify_quality`
* **Địa chỉ kiểm tra:**
  - Airflow Web UI: `http://localhost:8080`
* **Bằng chứng cần chụp/lưu:**
  - Ảnh chụp **Graph View** hoặc **Grid View** với tất cả các tasks đều xanh lục (Success).
  - Log xác thực của Task 4 (`verify_quality`): đọc Kafka State Topics và xác nhận đúng 55 spots, có model K-Means và lộ trình Itinerary.

---

### PHẦN 6. BẰNG CHỨNG GIAO DIỆN WEB DASHBOARD (SERVING LAYER)

* **Địa chỉ Web:** `http://localhost:5000`
* **Kiểm tra Serving Layer trong RAM (Không Database):**
  1. Trạng thái HTTP 202 khi gửi đánh giá: UI hiển thị `"⏳ Đang xử lý qua Kafka Speed Layer..."`.
  2. SSE Stream đẩy từ `tourism-output`: Điểm số thẻ địa điểm cập nhật lập tức trong vòng < 2 giây.
  3. Khi tắt Kafka Broker: Gửi đánh giá trả về HTTP 503 và thông báo rõ ràng `"Kafka Broker ngoại tuyến"`.
  4. Nút "Đồng bộ Batch Layer": Gọi ngầm `spark_batch_train.py` với khóa `threading.Lock()` chống xung đột, cập nhật trạng thái qua SSE và polling `/api/sync_status`.

