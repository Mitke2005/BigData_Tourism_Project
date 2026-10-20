# ĐỒ ÁN BIG DATA: KIẾN TRÚC LAMBDA KAFKA-FIRST PHÂN TÍCH DỮ LIỆU DU LỊCH & GỢI Ý LỊCH TRÌNH THÔNG MINH

## 1. Tổng quan Kiến trúc Lambda Kafka-First (Kafka-First Lambda Architecture)

Dự án áp dụng kiến trúc **Kafka-First Lambda Architecture** chuẩn hóa nhằm giải quyết bài toán phân tích dữ liệu lớn du lịch và gợi ý lịch trình thông minh, kết hợp giữa xử lý khối lượng lớn định kỳ (Batch Layer) và xử lý luồng sự kiện thời gian thực (Speed Layer), thống nhất tại kho phục vụ trong bộ nhớ (Serving Layer) — **100% không phụ thuộc SQLite/RDBMS**:

```text
                               ┌────────────────────────────────────────────────────────┐
                               │                    NGUỒN DỮ LIỆU                       │
                               │   Tập dữ liệu 317 danh lam thắng cảnh Việt Nam (HDFS)   │
                               │   (Khóa SHA-256 bất biến, không bao giờ bị ghi đè)     │
                               └───────────────────────────┬────────────────────────────┘
                                                           │
                      ┌────────────────────────────────────┴───────────────────────────────────┐
                      │                                                                        │
                      ▼ (Batch định kỳ / Airflow)                                              ▼ (Luồng thời gian thực)
       ┌──────────────────────────────┐                                         ┌──────────────────────────────┐
       │         BATCH LAYER          │                                         │         SPEED LAYER          │
       │ - Hadoop HDFS (Port 9000)    │                                         │ - Apache Kafka 3 Partitions  │
       │ - PySpark MLlib (K-Means,    │                                         │   (Topic: tourism-input)     │
       │   TF-IDF, FP-Growth)         │                                         │ - Spark Structured Streaming │
       │ - Parquet Data Lake          │                                         │   (Micro-batch trigger 5s,   │
       │   (Analytics Zone, Snappy)   │                                         │    checkpoint HDFS)          │
       └──────────────┬───────────────┘                                         └──────────────┬───────────────┘
                      │                                                                        │
                      │ [Publish Compacted State Topics]                                       │ [Publish Real-time Topics]
                      ▼                                                                        ▼
       ┌───────────────────────────────────────────────────────────────────────────────────────────────────────┐
       │                                     KAFKA STATE & STREAMING TOPICS                                    │
       │             - tourism-state-spots     : 317 spots & rating snapshot (cleanup.policy=compact)           │
       │             - tourism-state-kmeans    : Tọa độ tâm cụm & metadata (cleanup.policy=compact)            │
       │             - tourism-state-itinerary : 55 lộ trình đa ngày tối ưu Haversine (compact)                │
       │             - tourism-control         : Commit metadata & Streaming Heartbeat                         │
       │             - tourism-reviews         : Nhật ký đánh giá thời gian thực (Speed Layer)                 │
       │             - tourism-output          : Sự kiện làm giàu cụm đẩy trực tiếp tới Web SSE                │
       └──────────────────────────────────────────────────┬────────────────────────────────────────────────────┘
                                                          │
                                                          ▼ [Khởi tạo & Đồng bộ liên tục vào RAM]
       ┌───────────────────────────────────────────────────────────────────────────────────────────────────────┐
       │                                            SERVING LAYER                                              │
       │                       In-Memory Materialized View (serving/kafka_materializer.py)                     │
       │             - Lưu toàn bộ State trong RAM (Dict/Set), KHÔNG dùng SQLite hay database ngoài            │
       │             - Merge Lambda View tức thì: updated_rating = (BatchSum + LiveSum) / TotalCount           │
       │             - Lọc trùng theo synced_event_ids và Idempotent theo event_id                             │
       └──────────────────────────────────────────────────┬────────────────────────────────────────────────────┘
                                                          │
                                                          ▼
                                       ┌──────────────────────────────────────┐
                                       │       WEB APPLICATION & SSE          │
                                       │   Flask Live Dashboard + Leaflet     │
                                       │   Real-time Server-Sent Events (SSE) │
                                       │   HTTP 202 Ingest / HTTP 503 Falback │
                                       └──────────────────────────────────────┘
```

---

## 2. Thứ tự khởi chạy chuẩn của hệ thống (Standard Run Sequence)

Để đảm bảo hệ thống vận hành đúng quy trình và không gặp lỗi thiếu tài nguyên/dịch vụ, hãy thực hiện theo đúng thứ tự 7 bước chuẩn:

### Bước 1: Khởi động Hadoop HDFS (Port 9000)
- **Windows**: Chạy PowerShell Administrator:
  ```powershell
  %HADOOP_HOME%\sbin\start-dfs.cmd
  ```
- **Linux / WSL**:
  ```bash
  $HADOOP_HOME/sbin/start-dfs.sh
  ```
- *Kiểm tra*: Mở trình duyệt truy cập NameNode UI tại `http://localhost:9870`.

### Bước 2: Khởi động Apache Kafka Broker (Port 9092)
- Khởi động Kafka KRaft mode hoặc Zookeeper + Kafka:
  ```powershell
  # Windows
  .\bin\windows\kafka-server-start.bat .\config\kraft\server.properties
  ```
  ```bash
  # Linux / WSL
  bin/kafka-server-start.sh config/kraft/server.properties
  ```

### Bước 3: Tạo các Kafka Topics (6 Topics)
- Tạo các topic phân vùng stream và compacted state topics:
  ```bash
  python messaging/create_topics.py
  ```
- *Kiểm tra*: `kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic tourism-input` (PartitionCount: 3).

### Bước 4: Chạy Job huấn luyện Batch Layer (`spark_batch_train.py`)
- Huấn luyện K-Means (k=5), TF-IDF, FP-Growth, tính toán lịch trình đa ngày và nạp vào Kafka State Store + HDFS Analytics Zone:
  ```bash
  python spark/spark_batch_train.py
  ```
- *Nghiệm thu*: Kafka State Store ghi nhận đầy đủ 317 spots, model views, itineraries, batch_commit.

### Bước 5: Khởi động Speed Layer (`spark_structured_streaming.py`)
- Lắng nghe sự kiện từ `tourism-input`, làm giàu dữ liệu qua K-Means Euclid, ghi Parquet HDFS và publish `tourism-output` / `tourism-reviews`:
  ```bash
  python spark/spark_structured_streaming.py
  ```

### Bước 6: Khởi động Web Dashboard (`web_dashboard/app.py`)
- Cung cấp giao diện tương tác, bản đồ Leaflet, SSE Stream và API Lambda Serving trong RAM:
  ```bash
  python web_dashboard/app.py
  ```
- Truy cập trình duyệt tại: `http://localhost:5000`

### Bước 7: Khởi động Producer phát luồng mô phỏng (`messaging/producer.py`)
- Mô phỏng người dùng xem địa điểm liên tục vào `tourism-input` (chờ heartbeat từ Spark Streaming):
  ```bash
  python messaging/producer.py
  ```

> **Mẹo tiện ích:** Có thể chạy toàn bộ kịch bản tự động bằng một lệnh:
> ```bash
> python run_all_demo.py
> ```
> `run_all_demo.py` sẽ tự động thực thi tuần tự: Kiểm tra dịch vụ -> Huấn luyện Batch (`spark_batch_train.py`) -> Khởi động Streaming -> Web -> Producer, và dọn dẹp sạch subprocesses khi bấm `Ctrl+C`.

---

## 3. Đặc tả Kỹ thuật & Độ bền Dữ liệu

### 3.1 Ngữ nghĩa phân phối & Độ bền (At-least-once + Idempotent Sink)
- Hệ thống áp dụng mô hình phân phối **At-Least-Once** kết hợp với **Xử lý Idempotent theo `event_id`**:
  1. **Kafka Input/Output**: Acks=`all`, Producer retries=3, Partition Key là `session_id` giúp bảo toàn thứ tự phiên.
  2. **HDFS Parquet Lake**: Ghi vi mô theo từng micro-batch tại `hdfs://localhost:9000/tourism_data/streaming_zone/events/run_id=.../batch_id=.../` với chế độ `mode("overwrite")`. Khi một batch gặp sự cố và chạy lại, nó sẽ ghi đè chính xác thư mục của batch đó, không tạo trùng lặp.
  3. **In-Memory Serving**: Hoàn toàn không dùng SQLite. Dữ liệu đánh giá được duy trì trong RAM với tập hợp `Set[event_id]` giúp chống cộng dồn trùng lặp khi Kafka replay tin nhắn.
  4. **Compaction Pipeline**: Job `spark/compact_streaming_zone.py` định kỳ gộp các batch Parquet nhỏ trong `streaming_zone/events` và `streaming_zone/reviews`, áp dụng `dropDuplicates(["event_id"])` trước khi ghi nén Snappy vào `analytics_zone`.
- Ngữ nghĩa chính xác là **At-Least-Once vận chuyển + Idempotent lưu trữ**.

### 3.2 Chu kỳ xử lý vi phân (Micro-batch Trigger)
- Spark Structured Streaming được cấu hình trigger vi phân chu kỳ 5 giây:
  ```python
  .trigger(processingTime="5 seconds")
  ```
- Độ trễ đầu-cuối (End-to-End Latency) trung bình đạt khoảng 1.5 - 5.5 giây phụ thuộc vào chu kỳ trigger và I/O mạng.

---

## 4. Danh mục Kịch bản Kiểm thử & Bằng chứng (Verification Scripts)

Hệ thống cung cấp sẵn các script kiểm định tự động trong thư mục `scratch/`:

| Kịch bản | Lệnh thực thi | Mục tiêu kiểm chứng |
| :--- | :--- | :--- |
| **Bảo toàn Master Dataset** | `python scratch/test_master_immutable.py` | Kiểm tra SHA-256 hash của Master CSV không bị sửa đổi (`d646ecc6...`) |
| **Idempotent Replay** | `python scratch/test_idempotent_replay.py` | Gửi cùng 1 `event_id` nhiều lần, kiểm tra review_count và rating không bị nhân đôi |
| **Phục hồi Restart** | `python scratch/test_restart_recovery.py` | Xóa RAM Materializer, khôi phục 100% Serving Layer từ Kafka State Streams mà không cần DB |
| **Dead-Letter Queue** | `python scratch/test_dead_letter.py` | Kiểm tra bắt lỗi JSON hỏng, STT sai, Rating ngoài khoảng [1..5], văn bản tràn |
| **Đồng bộ Batch Sync** | `python scratch/test_sync_batch.py` | Xác thực vòng lặp Closed-Loop: `synced_event_ids` loại trừ đúng, review_count không bị về 0 |
| **Kiểm thử E2E Serving** | `python scratch/test_kafka_first_e2e.py` | Kiểm tra toàn trình Batch views + Real-time reviews phục vụ hoàn toàn trong bộ nhớ RAM |
| **Gộp file nhỏ** | `python spark/compact_streaming_zone.py --older-than-minutes 10` | Gộp Parquet nhỏ trong streaming zone sang analytics zone, khử trùng `event_id` |

*Chi tiết danh mục ảnh chụp và câu lệnh báo cáo được trình bày tại [docs/EVIDENCE_CHECKLIST.md](file:///d:/BigData/DOAN/docs/EVIDENCE_CHECKLIST.md).*
