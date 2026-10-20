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


class TestKafkaFirstE2E(unittest.TestCase):

    def test_full_e2e_serving_in_memory(self):
        """Kiểm tra toàn trình (E2E) phục vụ Lambda View trong bộ nhớ: Batch Views + Real-time Reviews."""
        mat = KafkaMaterializer()

        # 1. Nạp danh mục địa điểm từ batch layer
        for i in range(1, 4):
            mat._process_single_message(TOPIC_STATE_SPOTS, str(i), {
                "stt": i,
                "name": f"Điểm đến {i}",
                "location": "Hà Nội" if i == 1 else "Đà Nẵng",
                "base_rating": 4.0,
                "rating": 4.0,
                "review_sum": 40.0,
                "review_count": 10,
                "cluster_id": 1,
                "cluster_name": "Di sản Văn hóa",
                "cluster_color": "#EF4444",
                "keywords": ["văn hóa", "lịch sử"],
                "synced_event_ids": []
            })
        mat._process_single_message(TOPIC_STATE_BATCH, "batch_commit", {
            "status": "COMMITTED",
            "job_id": "batch_2026",
            "timestamp": "2026-10-10"
        })

        # 2. Nạp mô hình KMeans
        mat._process_single_message(TOPIC_STATE_BATCH, "model:kmeans", {
            "k": 5,
            "centers": [[21.0, 105.8], [16.0, 108.2]],
            "version": "v1",
            "cluster_meta": {
                "1": {"cluster_name": "Di sản Văn hóa", "cluster_color": "#EF4444"}
            }
        })

        # 3. Nạp lộ trình đa ngày
        mat._process_single_message(TOPIC_STATE_BATCH, "itinerary:hanoi:1", {
            "destination": "hanoi",
            "day": 1,
            "spots": [1],
            "total_distance_km": 5.2
        })

        # 4. Kiểm tra dữ liệu khi chưa có review mới
        all_spots_initial = mat.get_lambda_spot_views()
        self.assertEqual(len(all_spots_initial), 3)
        self.assertEqual(all_spots_initial[0]["rating"], 4.0)
        self.assertEqual(all_spots_initial[0]["review_count"], 10)

        # 5. Speed layer: người dùng đánh giá điểm đến số 1 (5 sao)
        mat._process_single_message(TOPIC_REVIEWS, "LIVE_EVT_1", {
            "event_id": "LIVE_EVT_1",
            "stt": 1,
            "rating": 5.0,
            "user_name": "Kỹ sư Big Data",
            "review_text": "Hệ thống Kafka-First chạy rất nhanh và ổn định!",
            "timestamp": "2026-10-10 12:00:00"
        })

        # 6. Xác thực Lambda Merged View cập nhật lập tức
        spot1_updated = mat.get_spot_lambda_view(1)
        self.assertEqual(spot1_updated["review_count"], 11)
        # Prior Bayes: (4.0*5 + 45) / (5 + 11) = 65 / 16 = 4.06
        self.assertEqual(spot1_updated["rating"], 4.06)

        # 7. Kiểm tra API mô hình và lộ trình
        kmeans_mod = mat.get_batch_model("kmeans")
        self.assertIsNotNone(kmeans_mod)
        self.assertEqual(kmeans_mod.get("k"), 5)

        itin = mat.get_batch_itinerary("hanoi", 1)
        self.assertIsNotNone(itin)
        self.assertEqual(itin.get("destination"), "hanoi")

        print("[+] [Test Kafka First E2E] Đạt: Hoàn tất kiểm thử E2E Serving hoàn toàn trong RAM không phụ thuộc SQLite!")


if __name__ == '__main__':
    unittest.main()
