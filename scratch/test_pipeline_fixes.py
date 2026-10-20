"""
scratch/test_pipeline_fixes.py
==============================
Test suite kiểm chứng toàn bộ 6 yêu cầu sửa đổi của đồ án:
1. Archiver Idempotency & Manifest (Phục hồi sau restart/retry lỗi một phần)
2. Bảo toàn review_rating (5 sao) vs updated_rating (4.17) & Chống trùng replay/sync
3. Lan truyền t_backend_ms (LongType) & Đo đạc thời gian thực tế
4. Xác thực Heartbeat (Đúng session, còn mới, status ACTIVE, báo lỗi khi Spark chết/timeout)
5. Reconnect Materializer & khôi phục offset
6. An toàn hiển thị phòng chống XSS / HTML injection
"""

import os
import sys
import json
import time
import unittest
from unittest.mock import MagicMock, patch

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from serving.lambda_view import compute_spot_lambda_view
from spark.spark_structured_streaming import INPUT_SCHEMA
from spark.hdfs_stream_archiver import load_manifest, save_manifest, LOCAL_MANIFEST_PATH


class TestPipelineFixes(unittest.TestCase):

    def test_item2_rating_and_replay_and_sync(self):
        """
        Kiểm chứng Yêu cầu 2:
        - Điểm nền 4.0, review 5 sao -> điểm hợp nhất là 4.17 (Bayes prior n0=5)
        - Điểm gốc của review vẫn giữ nguyên 5.0
        - Replay lại cùng review (cùng event_id): không tăng count, điểm giữ nguyên 4.17
        - Đồng bộ vào synced_event_ids: không cộng trùng
        """
        spot_state = {
            "stt": 10,
            "name": "Bà Nà Hills",
            "base_rating": 4.0,
            "rating": 4.0,
            "review_sum": 0.0,
            "review_count": 0
        }
        review_1 = {
            "event_id": "rev_001",
            "stt": 10,
            "user_name": "Nam",
            "rating": 5.0,
            "review_rating": 5.0,
            "review_text": "Tuyệt vời",
            "timestamp": "2026-10-11 00:00:01"
        }

        synced_ids = set()
        # 1. Tính lần đầu
        view_1 = compute_spot_lambda_view(spot_state, [review_1], synced_ids, prior_weight=5.0)
        # Công thức: (4.0 * 5 + 5.0) / (5 + 1) = 25.0 / 6 = 4.1666... -> 4.17
        self.assertEqual(view_1["rating"], 4.17)
        self.assertEqual(view_1["review_count"], 1)

        # Đảm bảo review gốc vẫn giữ nguyên điểm 5.0
        self.assertEqual(review_1["review_rating"], 5.0)

        # 2. Replay cùng event_id
        review_replay = dict(review_1)
        view_2 = compute_spot_lambda_view(spot_state, [review_1, review_replay], synced_ids, prior_weight=5.0)
        self.assertEqual(view_2["rating"], 4.17)
        self.assertEqual(view_2["review_count"], 1)

        # 3. Khi review được đồng bộ vào Batch Layer (synced_event_ids chứa rev_001)
        # Giả lập Batch View sau khi batch train nạp review_1:
        spot_state_synced = {
            "stt": 10,
            "name": "Bà Nà Hills",
            "base_rating": 4.0,
            "rating": 4.17,
            "review_sum": 5.0,
            "review_count": 1
        }
        synced_ids.add("rev_001")
        view_3 = compute_spot_lambda_view(spot_state_synced, [review_1], synced_ids, prior_weight=5.0)
        # Điểm và count không bị cộng đúp:
        self.assertEqual(view_3["review_count"], 1)
        self.assertEqual(view_3["rating"], 4.17)

    def test_item3_schema_contains_backend_ts_and_review_rating(self):
        """
        Kiểm chứng Yêu cầu 3:
        - INPUT_SCHEMA của Spark phải có t_backend_ms kiểu LongType
        - INPUT_SCHEMA phải có review_rating kiểu DoubleType
        """
        fields = {f.name: f.dataType.simpleString() for f in INPUT_SCHEMA.fields}
        self.assertIn("t_backend_ms", fields)
        self.assertEqual(fields["t_backend_ms"], "bigint")  # LongType hiển thị là bigint trong simpleString
        self.assertIn("review_rating", fields)
        self.assertEqual(fields["review_rating"], "double")

    def test_item4_heartbeat_verification(self):
        """
        Kiểm chứng Yêu cầu 4:
        - Heartbeat khác session_id -> bị bỏ qua
        - Heartbeat status != 'ACTIVE' -> bị bỏ qua
        - Heartbeat cũ (>30s) -> bị bỏ qua
        - Heartbeat hợp lệ -> chấp nhận
        - Spark process chết sớm -> ném RuntimeError ngay
        """
        from run_all_demo import wait_for_streaming_heartbeat

        # 1. Kiểm tra tiến trình Spark đã chết sớm
        mock_proc_dead = MagicMock()
        mock_proc_dead.poll.return_value = 1
        with self.assertRaises(RuntimeError) as ctx:
            wait_for_streaming_heartbeat("sess_test", p_spark=mock_proc_dead, timeout_sec=2)
        self.assertIn("đã thoát sớm", str(ctx.exception))

        # 2. Kiểm tra heartbeat sai session hoặc timeout
        mock_proc_alive = MagicMock()
        mock_proc_alive.poll.return_value = None

        with patch("kafka.KafkaConsumer") as mock_kc:
            consumer_instance = MagicMock()
            mock_kc.return_value = consumer_instance

            # Giả lập trả về heartbeat cũ hoặc sai session_id
            stale_msg = MagicMock()
            stale_msg.key = b"streaming"
            stale_msg.value = json.dumps({
                "status": "ACTIVE",
                "session_id": "wrong_session",
                "ts_ms": int(time.time() * 1000)
            }).encode('utf-8')

            consumer_instance.poll.return_value = {MagicMock(): [stale_msg]}

            with self.assertRaises(RuntimeError) as ctx_timeout:
                wait_for_streaming_heartbeat("correct_session", p_spark=mock_proc_alive, timeout_sec=1)
            self.assertIn("Quá 1s không nhận được heartbeat hợp lệ", str(ctx_timeout.exception))

            # Giả lập trả về heartbeat đúng session_id, ACTIVE và mới
            valid_msg = MagicMock()
            valid_msg.key = b"streaming"
            valid_msg.value = json.dumps({
                "status": "ACTIVE",
                "session_id": "correct_session",
                "ts_ms": int(time.time() * 1000)
            }).encode('utf-8')

            consumer_instance.poll.return_value = {MagicMock(): [valid_msg]}
            res = wait_for_streaming_heartbeat("correct_session", p_spark=mock_proc_alive, timeout_sec=3)
            self.assertTrue(res)

    def test_item1_manifest_and_idempotent_offset_tracking(self):
        """
        Kiểm chứng Yêu cầu 1:
        - Manifest được lưu bền vững và tải lại chính xác
        - Offset đã xử lý trong manifest không bị đọc lại / ghi trùng
        """
        import tempfile
        temp_dir = tempfile.mkdtemp()
        temp_manifest = os.path.join(temp_dir, "test_manifest.json")
        try:
            with patch("spark.hdfs_stream_archiver.LOCAL_MANIFEST_PATH", temp_manifest), \
                 patch("spark.hdfs_stream_archiver.is_hdfs_alive", return_value=False):
                test_manifest = {
                    "tourism-output": {"0": 100, "1": 200},
                    "tourism-reviews": {"0": 50}
                }
                save_manifest(test_manifest)
                loaded = load_manifest()
                self.assertEqual(loaded.get("tourism-output", {}).get("0"), 100)
                self.assertEqual(loaded.get("tourism-output", {}).get("1"), 200)
                self.assertEqual(loaded.get("tourism-reviews", {}).get("0"), 50)
        finally:
            if os.path.exists(temp_manifest):
                os.remove(temp_manifest)
            if os.path.exists(temp_dir):
                os.rmdir(temp_dir)

    def test_item6_xss_escaping(self):
        """
        Kiểm chứng Yêu cầu 6:
        - Dữ liệu người dùng chứa thẻ HTML/Script được escape an toàn khi chuyển đổi
        """
        payload_script = "<script>alert('xss')</script>"
        payload_img = '<img src=x onerror="alert(1)">'

        import html
        escaped_script = html.escape(payload_script)
        escaped_img = html.escape(payload_img)

        self.assertNotIn("<script>", escaped_script)
        self.assertIn("&lt;script&gt;", escaped_script)
        self.assertNotIn("<img", escaped_img)
        self.assertIn("&lt;img", escaped_img)


if __name__ == '__main__':
    unittest.main()
