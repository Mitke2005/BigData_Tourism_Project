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
from messaging.state_store import TOPIC_STATE_SPOTS, TOPIC_REVIEWS


class TestIdempotentReplay(unittest.TestCase):

    def setUp(self):
        self.mat = KafkaMaterializer()
        self.mat._process_single_message(
            TOPIC_STATE_SPOTS,
            "1",
            {
                "stt": 1,
                "name": "Hồ Hoàn Kiếm",
                "location": "Hà Nội",
                "base_rating": 4.0,
                "rating": 4.0,
                "review_sum": 40.0,
                "review_count": 10,
                "synced_event_ids": []
            }
        )

    def test_idempotent_review_ingestion(self):
        """Kiểm tra: Nạp review có event_id=EVT_100 nhiều lần chỉ được tính 1 lần duy nhất."""
        review_event = {
            "event_id": "EVT_100",
            "stt": 1,
            "rating": 5.0,
            "user_name": "Nguyen Van A",
            "review_text": "Rất đẹp!",
            "timestamp": "2026-10-10 12:00:00"
        }

        # Lần 1: Nạp review
        self.mat._process_single_message(TOPIC_REVIEWS, "EVT_100", review_event)
        v1 = self.mat.get_spot_lambda_view(1)
        self.assertEqual(v1["review_count"], 11)
        rating_1 = v1["rating"]

        # Lần 2: Nạp LẠI CHÍNH XÁC review_event (Idempotent replay)
        self.mat._process_single_message(TOPIC_REVIEWS, "EVT_100", review_event)
        v2 = self.mat.get_spot_lambda_view(1)
        self.assertEqual(v2["review_count"], 11, "Idempotency thất bại: review_count bị tăng khi replay!")
        self.assertEqual(v2["rating"], rating_1, "Idempotency thất bại: rating bị thay đổi khi replay!")

        # Lần 3: Replay lần nữa
        self.mat._process_single_message(TOPIC_REVIEWS, "EVT_100", review_event)
        v3 = self.mat.get_spot_lambda_view(1)
        self.assertEqual(v3["review_count"], 11)
        self.assertEqual(v3["rating"], rating_1)
        
        # Danh sách reviews chỉ có đúng 1 bản ghi
        reviews_data = self.mat.get_spot_reviews(1)
        self.assertEqual(reviews_data["total_reviews"], 1)
        print("[+] [Test Idempotent Replay] Đạt: Replay 3 lần cùng 1 event_id chỉ tính duy nhất 1 lần!")


if __name__ == '__main__':
    unittest.main()
