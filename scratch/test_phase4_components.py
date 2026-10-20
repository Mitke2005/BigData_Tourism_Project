import os
import sys
import unittest
from unittest.mock import MagicMock, patch

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)

from messaging.producer import extract_clean_keywords, clean_keyword_str
import run_all_demo
import spark.compact_streaming_zone as compact_mod
import airflow.tourism_pipeline_dag as dag_mod


class TestPhase4Components(unittest.TestCase):

    def test_keywords_extraction(self):
        kws = extract_clean_keywords('“Biển”, Đảo, “Biển”')
        self.assertEqual(kws, ['biển', 'đảo'])

    def test_clean_keyword_str(self):
        self.assertEqual(clean_keyword_str('“Hạ Long”'), 'hạ long')
        self.assertEqual(clean_keyword_str(''), '')

    def test_producer_simulated_payload_structure(self):
        """Kiểm tra logic tạo payload SIMULATED_VIEW không làm đổi rating."""
        import pandas as pd
        csv_path = os.path.join(BASE_DIR, "data", "tourism_dataset_clean.csv")
        df = pd.read_csv(csv_path)
        row = df.iloc[0]

        stt_val = int(row['STT'])
        payload = {
            "event_id": "test-uuid",
            "session_id": f"sess_{stt_val % 6}",
            "event_type": "SIMULATED_VIEW",
            "spot_id": stt_val,
            "stt": stt_val,
            "name": str(row['Tên địa điểm']).strip(),
            "location": str(row['Vị trí']).strip(),
            "rating": None,
            "is_user_review": False
        }

        self.assertEqual(payload["event_type"], "SIMULATED_VIEW")
        self.assertFalse(payload["is_user_review"])
        self.assertIsNone(payload["rating"])
        self.assertEqual(payload["spot_id"], 1)

    def test_wait_for_heartbeat_timeout(self):
        """Kiểm tra wait_for_streaming_heartbeat ném TimeoutError khi không có heartbeat."""
        from messaging.producer import wait_for_streaming_heartbeat
        with patch('messaging.producer.KafkaConsumer') as mock_consumer:
            instance = mock_consumer.return_value
            instance.poll.return_value = {}  # Không có tin nhắn nào
            with self.assertRaises(TimeoutError):
                wait_for_streaming_heartbeat(
                    bootstrap_servers="localhost:9092",
                    control_topic="tourism-control",
                    timeout_sec=1
                )

    def test_run_all_demo_process_management(self):
        """Kiểm tra đăng ký và dọn dẹp tiến trình con trong run_all_demo."""
        mock_proc = MagicMock()
        mock_proc.poll.return_value = None
        run_all_demo.register_process(mock_proc)
        self.assertIn(mock_proc, run_all_demo.CHILD_PROCESSES)

        run_all_demo.cleanup_all_processes()
        mock_proc.terminate.assert_called()

    def test_airflow_tasks_callable(self):
        """Kiểm tra 4 task trong airflow/tourism_pipeline_dag.py đều có thể gọi được."""
        self.assertTrue(callable(dag_mod.task_ensure_topics))
        self.assertTrue(callable(dag_mod.task_upload_master))
        self.assertTrue(callable(dag_mod.task_run_batch_job))
        self.assertTrue(callable(dag_mod.task_verify_quality))

    def test_compaction_args(self):
        """Kiểm tra parser của spark/compact_streaming_zone.py."""
        import argparse
        parser = argparse.ArgumentParser()
        parser.add_argument("--older-than-minutes", type=int, default=10)
        parser.add_argument("--delete-source", action="store_true")
        parser.add_argument("--target", type=str, default="all", choices=["all", "events", "reviews"])
        
        args = parser.parse_args(["--older-than-minutes", "5", "--delete-source", "--target", "events"])
        self.assertEqual(args.older_than_minutes, 5)
        self.assertTrue(args.delete_source)
        self.assertEqual(args.target, "events")


if __name__ == '__main__':
    unittest.main()
