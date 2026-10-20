# Bổ sung đánh giá theo bài giảng Kafka và Airflow

Ngày: 09/10/2026. Nguồn: `Kafka_Airflow_Lecture_Note_2026_revised_v2.pdf`, 52 trang, đang mở trong LMS của người dùng. Đã lưu bản PDF ảnh đủ 52 trang từ trình xem; không tải được tệp PDF gốc bằng công cụ trình duyệt hiện có. Đọc OCR toàn bộ và kiểm tra trực quan các trang 39, 44, 45, 46 cùng trang đầu/cuối. Đây là đối chiếu tài liệu, không phải một lần nghiệm thu hệ thống mới.

## 1. Những yêu cầu rõ hơn từ bài mới

| Trang | Nội dung bài giảng | Áp dụng cho đồ án du lịch |
|---|---|---|
| 3–5, 37 | Nguồn → Kafka → xử lý → lưu trữ liên tục; Airflow điều phối bên ngoài | Chia nhánh chuẩn bị catalog batch và nhánh tương tác streaming. `Dataset → HDFS → Spark → Kafka → Web` chỉ thể hiện batch/output bus, chưa đủ nhánh Kafka input |
| 9–12, 20–22 | Partition key, consumer group, offset, retention, replay | Dùng session ID làm key cho tương tác cùng phiên; minh chứng topic/partition/offset và phục hồi sau khi consumer dừng |
| 29, 33, 40–41 | Airflow chạy task hữu hạn; không đặt consumer chạy mãi vào task thường | DAG kiểm tra dịch vụ, chuẩn bị dữ liệu, quality gate, báo cáo; Spark streaming chạy độc lập |
| 39 | Spark `readStream`/`writeStream`, transform và checkpoint bền vững | Consumer Python hoặc Spark batch riêng chưa thay thế được yêu cầu Spark Structured Streaming |
| 44 | Thử dừng Spark, sink, Kafka; dữ liệu sai và xử lý trùng; Airflow down không nhất thiết dừng stream | Chứng minh backlog/recovery, sink lỗi không báo thành công, dedup; không tự khẳng định exactly-once cho mọi sink |
| 45 | Topic 2–3 partition; producer JSON; Spark streaming có ít nhất một transform; persistent sink; DAG 2–4 task hữu hạn có dependency/retry/validation; Compose/README; demo và failure test | Checklist cụ thể để nhóm hoàn thiện. Persistent sink được nêu là Cassandra/PostgreSQL hoặc DB được chấp thuận; giữ HDFS theo yêu cầu riêng thầy và xác nhận lựa chọn serving DB |
| 46 | Rubric gợi ý: Kafka 20%, producer/consumer 15%, Spark streaming 20%, Airflow 20%, persistence/validation 10%, Docker/docs 10%, demo/explanation 5% | Đây là rubric gợi ý trong bài, không phải điểm số thực tế của đồ án |
| 51–52 | Parse DAG thành công chưa chứng minh scheduler hoặc task chạy thật | Nộp run history, log, health và output; không chỉ ảnh parse OK |

## 2. Kết luận đối với DOAN và v2

**DOAN:** Các lỗi trong `DANH_GIA_DOAN_FLOW_20261009.md` vẫn còn nguyên ý nghĩa. File có tên Spark Streaming thực chất chạy KafkaConsumer + scikit-learn; lưu Parquet local chưa chứng minh HDFS; DAG print/return chưa điều phối job thật; web tự phát kết quả có thể che lỗi Kafka. Bài mới làm rõ rằng các phần này phải được nối và kiểm chứng thực tế.

**v2 hiện có:** `v2/travel_v2/stream_worker.py` dùng confluent-kafka Consumer, xác nhận HDFS raw/gold rồi commit SQLite và Kafka offset. Đây là nền tảng streaming Python có cơ chế lưu/phục hồi, nhưng chưa phải Spark Structured Streaming. Spark/YARN batch riêng không đủ cho tiêu chí Spark streaming ở trang 45. `v2/dags/travel_national.py` đã gọi các bước register/stage/Spark/gate/publish thật, có retry/timeout; có thể giữ làm nhánh batch thay vì thay bằng DAG mock.

Worker v2 hiện quản lý checkpoint/offset cho partition 0. **Không chỉ tăng số partition topic lên 3** rồi dùng nguyên worker: phải chuyển đọc và lưu tiến độ theo từng partition, bảo toàn thứ tự cùng session và kiểm chứng phục hồi. Nếu chuyển sang Structured Streaming, tiến độ source phục hồi dựa trên Spark checkpoint; không bê nguyên cơ chế consumer group commit vào làm checkpoint của Spark.

## 3. Flow đề xuất để trình bày

```text
NHÁNH BATCH:
Dataset du lịch → HDFS raw → Spark batch làm sạch/catalog/model
                → HDFS silver/gold → catalog phục vụ gợi ý

NHÁNH STREAMING CẦN HOÀN THIỆN:
App/producer replay có ghi nhãn nguồn → Kafka input (key=session_id)
 → Spark Structured Streaming: parse/validate/phân tích tương tác
 → HDFS raw/gold + kho kết quả bền vững
 → Web hiển thị gợi ý/lịch trình và giải thích điểm

Airflow: kiểm dịch vụ → chạy job hữu hạn → quality gate → báo cáo
         (đứng ngoài luồng từng sự kiện)
Kafka output: tùy chọn nếu Web cần consumer kết quả riêng.
```

ASP.NET MVC là lựa chọn UI, không phải yêu cầu bắt buộc của bài này. Nếu báo cáo dùng ASP.NET phải có code và app ASP.NET thật. Streamlit/Flask có thể dùng để chứng minh phân tích, nhưng cần ghi đúng công nghệ đang chạy.

## 4. Sáu công đoạn nhóm cần hoàn thiện theo thứ tự

1. **Chốt nền tảng và hợp đồng sự kiện:** giữ dữ liệu có provenance của v2; tách input thật/replay; thống nhất event/request/session ID, schema, timestamps, partition key; thiết kế topic 2–3 partition và checkpoint theo partition.
2. **Làm Spark streaming thật:** Kafka source, ít nhất parse + validation + một phép phân tích tương tác, query progress và checkpoint trên volume bền vững. Chỉ giữ một engine xử lý output online để tránh kết quả hai nhánh khác nhau.
3. **Nối sink và gợi ý:** ghi HDFS thực; lưu serving output theo event/request ID, chống trùng; xử lý dữ liệu sai và sink outage. Dùng catalog/model chuẩn bị sẵn để giảm RAM; không tính cosine mọi cặp toàn quốc.
4. **Nối Web đúng output:** chờ kết quả cùng request ID; thể hiện trạng thái gửi/đang xử lý/thành công/lỗi, timestamp, nguồn dữ liệu; lịch trình cùng vùng, không lặp điểm, thời gian và khoảng cách có giải thích. Không tự tạo output giả khi broker lỗi.
5. **Airflow và thử lỗi:** giữ DAG batch thật; thêm hoặc tách DAG điều khiển streaming hữu hạn nếu cần. Chụp run/log/retry/validation failure; dừng consumer rồi tiếp tục input, phục hồi từ checkpoint, kiểm output không mất/trùng; thử sink outage và malformed event.
6. **Đóng gói/nghiệm thu:** Compose và README từ máy sạch, pin phiên bản tương thích, kiểm ports/listeners/volumes; lưu bằng chứng trace input → Kafka → Spark → HDFS/DB → Web. Đo latency, lag, RAM; không dùng metric cố định thay kết quả đo.

Đây là sáu nhóm công việc, không có nghĩa sáu thao tác nhỏ hoặc hoàn tất trong một ngày. Chưa có đủ bằng chứng để ước lượng phần trăm hoàn thành theo rubric mới.

## 5. Cách phù hợp máy 8 GB

Huấn luyện và batch trước; khi demo chỉ chạy Kafka, HDFS, một Spark streaming local nhỏ và Web. Dừng YARN/Airflow batch sau khi lưu minh chứng, chạy Airflow riêng để chứng minh DAG. Trang 44 xác nhận luồng Kafka/Spark có thể tiếp tục khi Airflow ngừng, còn điều phối/lịch kiểm tra tạm dừng. Nếu cần chứng minh Airflow giám sát cùng lúc thì phải dành một phiên chạy riêng có đủ RAM.

Trang 16/30 lưu ý nhu cầu RAM của Airflow/full stack. Cần đo thực trên máy; không khẳng định full stack nhẹ chỉ vì giới hạn JVM heap. Không cài thêm Cassandra/Spark cluster/Redis chỉ để tăng số container nếu sink và chế độ local đã đáp ứng mục tiêu được thầy chấp thuận.

Không sao chép nguyên lệnh Mac/Colima sang Windows. Phiên bản image trong slide là cấu hình lab của giảng viên, không tự động nâng stack đang chạy chỉ để trùng số phiên bản. Trang 42 ghi rõ service map là sơ đồ, chưa phải Compose chạy ngay.

## 6. Tệp để gửi nhóm

- Bản PDF 52 trang: `Kafka_Airflow_Lecture_Note_2026_revised_v2_Ban_luu_52_trang.pdf` tại thư mục gốc BigData.
- Bản lưu chuẩn: `output/pdf/Kafka_Airflow_Lecture_Note_2026_revised_v2_ban_luu_tu_trinh_xem.pdf`.
- Loại bản sao: PDF ảnh, giữ thứ tự trang; không có text layer, liên kết gốc hay chất lượng vector. Không trình bày đây là PDF gốc tải từ LMS.
- Đã kiểm 52 ảnh khác nhau, PDF 52 trang và render lại trang đầu/cuối. Chi tiết hash từng trang: `tmp/kafka_airflow_pdf_export/manifest.json`.

Chưa sửa mã nguồn, chưa chạy lại toàn bộ stack, chưa cập nhật ZIP bàn giao trong lượt lấy tài liệu này.
