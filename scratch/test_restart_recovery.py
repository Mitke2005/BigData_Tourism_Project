import os
import sys
import unittest

if sys.platform.startswith('win'):
    try:
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
        sys.stderr.reconfigure(encoding='utf-8', errors='replace')
    except Exception:
        pass

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)

from serving.kafka_materializer import KafkaMaterializer
from messaging.state_store import TOPIC_STATE_SPOTS, TOPIC_STATE_BATCH, TOPIC_REVIEWS


class TestRestartRecovery(unittest.TestCase):

    def test_in_memory_recovery_from_state_stream(self):
        """Kiểm tra khôi phục trạng thái hoàn toàn từ State Records khi RAM bị xóa (mô phỏng Web Server khởi động lại)."""
        mat1 = KafkaMaterializer()

        catalog_records = [
            ("1", {
                "stt": 1,
                "name": "Hồ Gươm",
                "location": "Hà Nội",
                "base_rating": 4.5,
                "rating": 4.5,
                "review_sum": 45.0,
                "review_count": 10,
                "synced_event_ids": []
            }),
            ("2", {
                "stt": 2,
                "name": "Chùa Cầu",
                "location": "Hội An",
                "base_rating": 4.8,
                "rating": 4.8,
                "review_sum": 24.0,
                "review_count": 5,
                "synced_event_ids": []
            })
        ]

        batch_records = [
            ("model:kmeans", {
                "k": 5,
                "centers": [[21.0, 105.8], [16.0, 108.2]],
                "version": "v1"
            }),
            ("batch_commit", {
                "status": "COMMITTED",
                "job_id": "run_1",
                "timestamp": "2026-10-10 10:00:00",
                "synced_event_ids": []
            })
        ]

        # Nạp vào instance 1
        for key, val in catalog_records:
            mat1._process_single_message(TOPIC_STATE_SPOTS, key, val)
        for key, val in batch_records:
            mat1._process_single_message(TOPIC_STATE_BATCH, key, val)

        mat1._process_single_message(TOPIC_REVIEWS, "REV_999", {
            "event_id": "REV_999",
            "stt": 1,
            "rating": 5.0,
            "user_name": "Nguyen Truong Nhu",
            "review_text": "Tuyệt vời",
            "timestamp": "2026-10-10 11:00:00"
        })

        view1 = mat1.get_spot_lambda_view(1)
        self.assertEqual(view1["review_count"], 11)

        # --- MÔ PHỎNG SỰ CỐ: WEB SERVER BỊ TẮT ĐỘT NGỘT & KHỞI ĐỘNG LẠI (INSTANCE MỚI) ---
        mat2 = KafkaMaterializer()
        self.assertEqual(len(mat2._committed_spots), 0, "Instance mới phải hoàn toàn trống RAM ban đầu.")

        # Replay lại stream từ Kafka compacted topics
        for key, val in catalog_records:
            mat2._process_single_message(TOPIC_STATE_SPOTS, key, val)
        for key, val in batch_records:
            mat2._process_single_message(TOPIC_STATE_BATCH, key, val)
        mat2._process_single_message(TOPIC_REVIEWS, "REV_999", {
            "event_id": "REV_999",
            "stt": 1,
            "rating": 5.0,
            "user_name": "Nguyen Truong Nhu",
            "review_text": "Tuyệt vời",
            "timestamp": "2026-10-10 11:00:00"
        })

        # Xác thực kết quả phục hồi khớp 100%
        view2 = mat2.get_spot_lambda_view(1)
        self.assertEqual(view1["rating"], view2["rating"])
        self.assertEqual(view1["review_count"], view2["review_count"])
        self.assertEqual(len(mat2.get_lambda_spot_views()), 2)
        kmeans_mod = mat2.get_batch_model("kmeans")
        self.assertIsNotNone(kmeans_mod)
        self.assertEqual(kmeans_mod.get("k"), 5)
        print("[+] [Test Restart Recovery] Đạt: Khôi phục 100% trạng thái Serving Layer từ Kafka State Streams mà không cần DB/SQLite!")


if __name__ == '__main__':
    unittest.main()
