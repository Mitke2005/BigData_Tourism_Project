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


class TestSyncBatch(unittest.TestCase):

    def test_batch_sync_closed_loop(self):
        """
        Kiểm tra vòng lặp đồng bộ Batch Sync của Lambda Architecture:
        1. Nhận 2 review live: 1 sao (E1) và 5 sao (E2).
        2. Công bố batch và đánh dấu 2 review đã đồng bộ (synced_event_ids).
        3. Điểm số và lượt đánh giá không bị về 0.
        4. Nhận tiếp review live thứ ba (E3) -> hợp nhất chính xác.
        """
        mat = KafkaMaterializer()

        # Trạng thái ban đầu: STT 1 có 0 review người dùng
        mat._process_single_message(TOPIC_STATE_SPOTS, "1", {
            "stt": 1,
            "name": "Địa điểm A",
            "location": "Hà Nội",
            "base_rating": 4.0,
            "rating": 4.0,
            "review_sum": 0.0,
            "review_count": 0,
            "synced_event_ids": []
        })

        # Bước 1: Nhận review live 1 sao (E1) và 5 sao (E2)
        mat._process_single_message(TOPIC_REVIEWS, "E1", {
            "event_id": "E1",
            "stt": 1,
            "rating": 1.0,
            "user_name": "User 1",
            "timestamp": "2026-10-10 10:00:00"
        })
        mat._process_single_message(TOPIC_REVIEWS, "E2", {
            "event_id": "E2",
            "stt": 1,
            "rating": 5.0,
            "user_name": "User 2",
            "timestamp": "2026-10-10 10:01:00"
        })

        v1 = mat.get_spot_lambda_view(1)
        self.assertEqual(v1["review_count"], 2)
        # Điểm Bayes: (4.0 * 5 + 6.0) / (5 + 2) = 26 / 7 = 3.71
        self.assertEqual(v1["rating"], 3.71)

        # Bước 2: Batch chạy xong, công bố batch view mới hấp thụ E1 & E2
        # batch spot mới với job_id: job_sync_1
        mat._process_single_message(TOPIC_STATE_SPOTS, "1", {
            "stt": 1,
            "job_id": "job_sync_1",
            "name": "Địa điểm A",
            "location": "Hà Nội",
            "base_rating": 4.0,
            "rating": 3.71,
            "review_sum": 6.0,
            "review_count": 2,
            "synced_event_ids": ["E1", "E2"]
        })
        mat._process_single_message(TOPIC_STATE_BATCH, "batch_commit", {
            "status": "COMMITTED",
            "job_id": "job_sync_1",
            "synced_event_ids": ["E1", "E2"]
        })

        v2 = mat.get_spot_lambda_view(1)
        # BẮT BUỘC: Không được bị reset về 0 lượt!
        self.assertEqual(v2["review_count"], 2, "LỖI HỒI QUY: Lượt đánh giá bị reset sau đồng bộ!")
        self.assertEqual(v2["rating"], 3.71, "LỖI HỒI QUY: Điểm số bị sai lệch sau đồng bộ!")

        # Bước 3: Nhận thêm review live 5 sao (E3) trong phiên mới
        mat._process_single_message(TOPIC_REVIEWS, "E3", {
            "event_id": "E3",
            "stt": 1,
            "rating": 5.0,
            "user_name": "User 3",
            "timestamp": "2026-10-10 10:05:00"
        })

        v3 = mat.get_spot_lambda_view(1)
        # Tổng lượt = 2 (từ batch) + 1 (live mới E3) = 3 lượt
        # Tổng điểm = 6.0 (batch) + 5.0 (E3) = 11.0. Rating Bayes: (4.0*5 + 11) / (5 + 3) = 31 / 8 = 3.88
        self.assertEqual(v3["review_count"], 3)
        self.assertEqual(v3["rating"], 3.88)
        print("[+] [Test Sync Batch] Đạt: Vòng khép kín Lambda Closed-Loop chính xác, không reset count về 0!")


if __name__ == '__main__':
    unittest.main()
