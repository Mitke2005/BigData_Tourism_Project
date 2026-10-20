"""
UNIT TEST THUẦN TÚY CHO CÔNG THỨC HỢP NHẤT LAMBDA VIEW
=============================================================================
Kiểm thử 5 kịch bản cốt lõi (Không cần Kafka / Database):
1. Không có review nào -> display_rating == base_rating, count == 0.
2. 1 review mới -> display_rating tính chính xác theo Bayes prior N0=5.
3. Nhiều review mới -> display_rating tính chính xác.
4. Replay cùng event_id -> Deduplicate tuyệt đối (idempotent), count không tăng.
5. Review thuộc synced_event_ids -> Không bị tính lặp 2 lần vào điểm tổng.
=============================================================================
"""

import os
import sys

if sys.platform.startswith('win'):
    try:
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
        sys.stderr.reconfigure(encoding='utf-8', errors='replace')
    except Exception:
        pass

# Thêm đường dẫn project
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from serving.lambda_view import compute_spot_lambda_view

def test_lambda_view():
    print("=" * 70)
    print("  CHẠY UNIT TEST CÔNG THỨC LAMBDA VIEW (serving/lambda_view.py)")
    print("=" * 70)

    base_spot = {
        "stt": 1,
        "name": "Bãi Dài Cam Ranh",
        "base_rating": 4.0,
        "review_sum": 0.0,
        "review_count": 0,
        "cluster_id": 2
    }

    # Test 1: Không có review nào
    res1 = compute_spot_lambda_view(base_spot, reviews_list=[], synced_event_ids=set(), prior_weight=5.0)
    assert res1["rating"] == 4.0, f"Test 1 Failed: Expected 4.0, got {res1['rating']}"
    assert res1["review_count"] == 0, f"Test 1 Failed: Expected count 0, got {res1['review_count']}"
    print("[+] Test 1 PASSED: Không có review -> display_rating == base_rating (4.0), count == 0.")

    # Test 2: 1 review mới (5 sao)
    # Bayes: (4.0 * 5 + 5.0) / (5 + 1) = 25.0 / 6 = 4.1666... -> 4.17
    rev_1 = [{"event_id": "rev_001", "rating": 5.0, "timestamp": "2026-10-10 10:00:00"}]
    res2 = compute_spot_lambda_view(base_spot, reviews_list=rev_1, synced_event_ids=set(), prior_weight=5.0)
    assert res2["rating"] == 4.17, f"Test 2 Failed: Expected 4.17, got {res2['rating']}"
    assert res2["review_count"] == 1, f"Test 2 Failed: Expected count 1, got {res2['review_count']}"
    print("[+] Test 2 PASSED: 1 review 5 sao -> display_rating = 4.17, count = 1.")

    # Test 3: Nhiều review (5 sao và 3 sao)
    # Bayes: (4.0 * 5 + 5.0 + 3.0) / (5 + 2) = 28.0 / 7 = 4.0
    rev_multi = [
        {"event_id": "rev_001", "rating": 5.0, "timestamp": "2026-10-10 10:00:00"},
        {"event_id": "rev_002", "rating": 3.0, "timestamp": "2026-10-10 10:05:00"}
    ]
    res3 = compute_spot_lambda_view(base_spot, reviews_list=rev_multi, synced_event_ids=set(), prior_weight=5.0)
    assert res3["rating"] == 4.0, f"Test 3 Failed: Expected 4.0, got {res3['rating']}"
    assert res3["review_count"] == 2, f"Test 3 Failed: Expected count 2, got {res3['review_count']}"
    print("[+] Test 3 PASSED: Nhiều review (5 sao + 3 sao) -> display_rating = 4.0, count = 2.")

    # Test 4: Replay cùng event_id rev_001 hai lần
    # Khử trùng theo event_id -> chỉ tính 1 review duy nhất
    rev_dup = [
        {"event_id": "rev_001", "rating": 5.0, "timestamp": "2026-10-10 10:00:00"},
        {"event_id": "rev_001", "rating": 5.0, "timestamp": "2026-10-10 10:00:00"}
    ]
    res4 = compute_spot_lambda_view(base_spot, reviews_list=rev_dup, synced_event_ids=set(), prior_weight=5.0)
    assert res4["review_count"] == 1, f"Test 4 Failed: Expected count 1, got {res4['review_count']}"
    assert res4["rating"] == 4.17, f"Test 4 Failed: Expected 4.17, got {res4['rating']}"
    print("[+] Test 4 PASSED: Replay cùng event_id -> Không tăng đếm (count=1, rating=4.17).")

    # Test 5: Review thuộc synced_event_ids không bị tính 2 lần
    # Batch view đã có sẵn 1 review (sum=5.0, count=1, chính là rev_001 đã sync)
    # Stream gửi lại rev_001 và thêm rev_003 (5 sao)
    batch_spot_synced = {
        "stt": 1,
        "name": "Bãi Dài Cam Ranh",
        "base_rating": 4.0,
        "review_sum": 5.0,
        "review_count": 1,
        "cluster_id": 2
    }
    rev_with_synced = [
        {"event_id": "rev_001", "rating": 5.0, "timestamp": "2026-10-10 10:00:00"}, # Đã nằm trong synced_event_ids
        {"event_id": "rev_003", "rating": 5.0, "timestamp": "2026-10-10 10:10:00"}  # Mới
    ]
    synced_set = {"rev_001"}
    res5 = compute_spot_lambda_view(batch_spot_synced, reviews_list=rev_with_synced, synced_event_ids=synced_set, prior_weight=5.0)
    # Bayes: (4.0 * 5 + 5.0 (batch_sum) + 5.0 (rev_003)) / (5 + 1 (batch_count) + 1 (rt_count)) = 30.0 / 7 = 4.2857... -> 4.29
    assert res5["review_count"] == 2, f"Test 5 Failed: Expected count 2, got {res5['review_count']}"
    assert res5["rating"] == 4.29, f"Test 5 Failed: Expected 4.29, got {res5['rating']}"
    assert res5["realtime_review_count"] == 1, f"Test 5 Failed: Expected rt_count 1, got {res5['realtime_review_count']}"
    print("[+] Test 5 PASSED: Review thuộc synced_event_ids không bị tính lặp 2 lần (count=2, rating=4.29).")

    print("\n[V] TẤT CẢ 5/5 UNIT TESTS CHO LAMBDA VIEW ĐỀU THÀNH CÔNG 100%!")

if __name__ == '__main__':
    test_lambda_view()
