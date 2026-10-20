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


class TestThreeCriticalFixes(unittest.TestCase):

    def test_fix_1_commit_before_spots_arrival(self):
        """
        VẤN ĐỀ 1: batch_commit đến trước khi nhận đủ dữ liệu.
        Kiểm tra: Khi batch_commit đến trước, hệ thống giữ ở pending;
        chỉ thăng cấp sang active khi tất cả các spot đến sau đã đủ.
        """
        mat = KafkaMaterializer()
        job_id = "job_early_commit_test"

        # 1. Gửi batch_commit TRƯỚC (khi chưa có bất kỳ spot nào)
        mat._process_single_message(TOPIC_STATE_BATCH, "batch_commit", {
            "status": "COMMITTED",
            "job_id": job_id,
            "spots_count": 2,
            "itinerary_count": 0,
            "synced_event_ids": []
        })

        # Phải chưa thăng cấp (active spots vẫn rỗng)
        self.assertEqual(len(mat._committed_spots), 0, "LỖI: Đã thăng cấp khi chưa có spot nào!")
        self.assertIn(job_id, mat._pending_commits)

        # 2. Spot 1 đến sau
        mat._process_single_message(TOPIC_STATE_SPOTS, "1", {
            "stt": 1,
            "job_id": job_id,
            "name": "Hồ Gươm",
            "rating": 4.5
        })
        # Vẫn chưa đủ 2 spots -> chưa thăng cấp
        self.assertEqual(len(mat._committed_spots), 0, "LỖI: Đã thăng cấp khi mới có 1/2 spots!")

        # 3. Spot 2 đến sau (Mảnh ghép cuối cùng)
        mat._process_single_message(TOPIC_STATE_SPOTS, "2", {
            "stt": 2,
            "job_id": job_id,
            "name": "Chùa Cầu",
            "rating": 4.8
        })

        # ĐÃ ĐỦ 2/2 SPOTS -> Tự động thăng cấp!
        self.assertEqual(len(mat._committed_spots), 2, "LỖI: Chưa thăng cấp dù đã đủ 2/2 spots!")
        self.assertNotIn(job_id, mat._pending_commits, "Commit phải được giải phóng khỏi pending!")
        self.assertEqual(mat.get_spot_lambda_view(1)["name"], "Hồ Gươm")
        self.assertEqual(mat.get_spot_lambda_view(2)["name"], "Chùa Cầu")
        print("[+] [Fix 1 PASS]: batch_commit đến trước được giữ pending an toàn, tự động thăng cấp khi nhận đủ spots!")

    def test_fix_2_itinerary_case_insensitivity(self):
        """
        VẤN ĐỀ 2: Tra cứu lịch trình bị lệch chữ hoa / chữ thường.
        Kiểm tra: Bất kể khi nạp dùng chữ hoa hay chữ thường (Khánh Hòa vs khánh hòa),
        khi tra cứu bằng bất kỳ dạng chữ hoa/thường nào đều trả về đúng lịch trình.
        """
        mat = KafkaMaterializer()

        # Nạp với key chữ hoa/thường hỗn hợp (như Spark từng xuất)
        mat._process_single_message(TOPIC_STATE_BATCH, "itinerary:Khánh Hòa:3", {
            "region": "Khánh Hòa",
            "days": 3,
            "itinerary": {
                "destination": "Khánh Hòa",
                "days": 3,
                "schedule": ["Vinpearl", "Tháp Bà", "Bình Ba"]
            }
        })

        # Tra cứu dạng chữ thường
        itin_lower = mat.get_batch_itinerary("khánh hòa", 3)
        self.assertIsNotNone(itin_lower, "LỖI: Không tìm thấy lịch trình khi tra cứu chữ thường 'khánh hòa'!")

        # Tra cứu dạng chữ hoa đầu từ
        itin_title = mat.get_batch_itinerary("Khánh Hòa", 3)
        self.assertIsNotNone(itin_title, "LỖI: Không tìm thấy lịch trình khi tra cứu 'Khánh Hòa'!")

        # Tra cứu dạng chữ hoa toàn bộ
        itin_upper = mat.get_batch_itinerary("KHÁNH HÒA", 3)
        self.assertIsNotNone(itin_upper, "LỖI: Không tìm thấy lịch trình khi tra cứu 'KHÁNH HÒA'!")

        # Tra cứu tìm kiếm mờ
        itin_fuzzy = mat.get_batch_itinerary("  khánh hòa  ", 3)
        self.assertIsNotNone(itin_fuzzy)
        print("[+] [Fix 2 PASS]: Tra cứu lịch trình hoàn toàn không phân biệt chữ hoa/thường, 100% khớp!")

    def test_fix_3_sse_review_freshness_and_race_prevention(self):
        """
        VẤN ĐỀ 3: SSE có thể phát điểm đánh giá cũ do race condition.
        Kiểm tra: Cơ chế ingest_live_review và has_review_event bảo đảm RAM Materializer
        luôn có review trước khi tính điểm SSE.
        """
        mat = KafkaMaterializer()
        mat._process_single_message(TOPIC_STATE_SPOTS, "1", {
            "stt": 1,
            "name": "Địa điểm A",
            "base_rating": 4.0,
            "rating": 4.0,
            "review_sum": 0.0,
            "review_count": 0,
            "synced_event_ids": []
        })

        # Giả sử sự kiện đến từ topic tourism-output trước khi tourism-reviews kịp tới
        sse_event = {
            "event_id": "EVT_SSE_RACE_TEST",
            "stt": 1,
            "rating": 5.0,
            "user_name": "Nguyen Truong Nhu",
            "is_user_review": True
        }

        # 1. Trước khi kiểm tra: Materializer chưa có event_id này
        self.assertFalse(mat.has_review_event("EVT_SSE_RACE_TEST"))

        # 2. Xử lý như sse_output_listener trong app.py:
        if not mat.has_review_event(sse_event["event_id"]):
            mat.ingest_live_review(sse_event)

        self.assertTrue(mat.has_review_event("EVT_SSE_RACE_TEST"))

        # 3. Lấy view: Điểm số BẮT BUỘC phải là điểm mới đã tính review này (không bao giờ là điểm cũ)
        spot_lambda = mat.get_spot_lambda_view(1)
        self.assertEqual(spot_lambda["review_count"], 1)
        # 1 review 5 sao với base 4.0: (4.0*5 + 5.0)/(5 + 1) = 25 / 6 = 4.17
        self.assertEqual(spot_lambda["rating"], 4.17)

        # 4. Khi consumer của tourism-reviews đọc tới sau đó, nạp lại cũng không bị nhân đôi (Idempotent)
        mat._process_single_message(TOPIC_REVIEWS, "EVT_SSE_RACE_TEST", sse_event)
        spot_lambda_after = mat.get_spot_lambda_view(1)
        self.assertEqual(spot_lambda_after["review_count"], 1)
        self.assertEqual(spot_lambda_after["rating"], 4.17)
        print("[+] [Fix 3 PASS]: Loại bỏ hoàn toàn race condition, SSE luôn phát điểm mới nhất và đảm bảo Idempotent!")

    def test_fix_4_review_rating_not_mutated_by_sse(self):
        """
        VẤN ĐỀ 4 (Lỗi còn lại): Điểm review gốc bị thay thành điểm tổng hợp do mutation.
        Kiểm tra:
        - Điểm nền 4.0, người dùng gửi 5 sao.
        - ingest_live_review() lưu bản sao độc lập (deepcopy).
        - Khi SSE thay đổi điểm hiển thị trên bản sao riêng hoặc trên biến event,
          review gốc trong RAM vẫn giữ nguyên 5 sao và API vẫn trả đúng 4.17.
        """
        import copy
        mat = KafkaMaterializer()
        mat._process_single_message(TOPIC_STATE_SPOTS, "1", {
            "stt": 1,
            "name": "Vịnh Hạ Long",
            "base_rating": 4.0,
            "rating": 4.0,
            "review_sum": 0.0,
            "review_count": 0,
            "synced_event_ids": []
        })

        # Người dùng gửi review 5 sao
        incoming_event = {
            "event_id": "EVT_MUTATION_CHECK_001",
            "stt": 1,
            "rating": 5.0,
            "user_name": "Nguyen Truong Nhu",
            "comment": "Rất đẹp!",
            "is_user_review": True
        }

        # 1. Nạp review vào Materializer
        mat.ingest_live_review(incoming_event)

        # 2. Điểm tổng hợp đúng lần 1: (4.0 * 5 + 5.0) / 6 = 4.17
        view1 = mat.get_spot_lambda_view(1)
        self.assertEqual(view1["rating"], 4.17)

        # 3. Giả lập quá trình SSE broadcast:
        # Dù SSE sửa event["rating"] hay sse_event["rating"] thành điểm tổng hợp 4.17:
        incoming_event["rating"] = view1["rating"]  # Cố tình sửa đối tượng ngoài thành 4.17

        # 4. Kiểm tra trong RAM: Review gốc trong RAM BẮT BUỘC vẫn phải là 5.0
        spot_reviews = mat.get_spot_reviews(1)
        self.assertEqual(len(spot_reviews["reviews"]), 1)
        stored_rating = spot_reviews["reviews"][0]["rating"]
        self.assertEqual(stored_rating, 5.0, f"LỖI: Review gốc trong RAM bị sửa thành {stored_rating} thay vì 5.0!")

        # 5. Kiểm tra API tính lại: Điểm tổng hợp vẫn phải là 4.17, TUYỆT ĐỐI không bị lệch thành 4.03
        view2 = mat.get_spot_lambda_view(1)
        self.assertEqual(view2["rating"], 4.17, f"LỖI: API tính lại bị sai thành {view2['rating']} (kỳ vọng 4.17)!")

        print("[+] [Fix 4 PASS]: Độc lập bộ nhớ hoàn toàn! SSE đổi điểm không làm ảnh hưởng review gốc (vẫn 5.0) và API vẫn trả đúng 4.17!")


if __name__ == '__main__':
    unittest.main()
