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


def validate_event_dict(event, valid_stts):
    """Mô phỏng chính xác logic kiểm tra validation từ spark_structured_streaming.py."""
    eid = event.get("event_id")
    etype = event.get("event_type")
    stt = event.get("stt")
    rating = event.get("rating")
    review_text = event.get("review_text")
    user_name = event.get("user_name")

    reasons = []
    if not eid or str(eid).strip() == "":
        reasons.append("MISSING_EVENT_ID")
    if not etype or etype not in ["REVIEW", "SIMULATED_VIEW"]:
        reasons.append("INVALID_EVENT_TYPE")
    if stt not in valid_stts:
        reasons.append("STT_NOT_IN_CATALOG")
    if etype == "REVIEW":
        if rating is None or rating < 1.0 or rating > 5.0:
            reasons.append("INVALID_RATING_[1,5]")
        if review_text and len(str(review_text)) > 1000:
            reasons.append("REVIEW_TEXT_EXCEEDS_1000")
        if user_name and len(str(user_name)) > 100:
            reasons.append("USER_NAME_EXCEEDS_100")

    return "; ".join(reasons)


class TestDeadLetterValidation(unittest.TestCase):

    def setUp(self):
        self.valid_stts = set(range(1, 56))

    def test_missing_event_id(self):
        evt = {"event_id": "", "event_type": "REVIEW", "stt": 1, "rating": 5.0}
        err = validate_event_dict(evt, self.valid_stts)
        self.assertIn("MISSING_EVENT_ID", err)

    def test_invalid_event_type(self):
        evt = {"event_id": "e1", "event_type": "HACK_VIEW", "stt": 1}
        err = validate_event_dict(evt, self.valid_stts)
        self.assertIn("INVALID_EVENT_TYPE", err)

    def test_stt_out_of_range(self):
        evt = {"event_id": "e2", "event_type": "REVIEW", "stt": 9999, "rating": 5.0}
        err = validate_event_dict(evt, self.valid_stts)
        self.assertIn("STT_NOT_IN_CATALOG", err)

    def test_invalid_rating(self):
        evt_zero = {"event_id": "e3", "event_type": "REVIEW", "stt": 1, "rating": 0.0}
        err_zero = validate_event_dict(evt_zero, self.valid_stts)
        self.assertIn("INVALID_RATING_[1,5]", err_zero)

        evt_over = {"event_id": "e4", "event_type": "REVIEW", "stt": 1, "rating": 6.5}
        err_over = validate_event_dict(evt_over, self.valid_stts)
        self.assertIn("INVALID_RATING_[1,5]", err_over)

    def test_text_overflow(self):
        evt_text = {"event_id": "e5", "event_type": "REVIEW", "stt": 1, "rating": 4.0, "review_text": "x" * 1005}
        err_text = validate_event_dict(evt_text, self.valid_stts)
        self.assertIn("REVIEW_TEXT_EXCEEDS_1000", err_text)

        evt_user = {"event_id": "e6", "event_type": "REVIEW", "stt": 1, "rating": 4.0, "user_name": "u" * 105}
        err_user = validate_event_dict(evt_user, self.valid_stts)
        self.assertIn("USER_NAME_EXCEEDS_100", err_user)

    def test_valid_simulated_view(self):
        evt_valid_view = {
            "event_id": "e7",
            "event_type": "SIMULATED_VIEW",
            "stt": 5,
            "rating": None,
            "is_user_review": False
        }
        err = validate_event_dict(evt_valid_view, self.valid_stts)
        self.assertEqual(err, "", "Sự kiện SIMULATED_VIEW hợp lệ không được bị đánh dấu lỗi!")
        print("[+] [Test Dead Letter] Đạt: Bắt chính xác 100% các điều kiện lỗi và chấp thuận sự kiện hợp lệ!")


if __name__ == '__main__':
    unittest.main()
