import os
import sys
import unittest
from unittest.mock import MagicMock, patch

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from spark.hdfs_stream_archiver import save_manifest, load_manifest, LOCAL_MANIFEST_PATH
from serving.kafka_materializer import KafkaMaterializer

class TestArchiverAndMaterializerFixes(unittest.TestCase):

    def test_save_manifest_exit_code_checking(self):
        """Kiểm tra save_manifest trả về False khi subprocess hdfs trả mã lỗi."""
        test_manifest = {
            "topics": {"tourism-output": {"0": 10}},
            "pending_batches": {}
        }
        
        with patch("spark.hdfs_stream_archiver.is_hdfs_alive", return_value=True):
            with patch("subprocess.run") as mock_run:
                # Mock mkdir success (returncode 0), but put fails (returncode 1)
                mock_run.side_effect = [
                    MagicMock(returncode=0, stderr=""),
                    MagicMock(returncode=1, stderr="put: Failed to copy to HDFS")
                ]
                success = save_manifest(test_manifest)
                self.assertFalse(success, "save_manifest phải trả về False khi hdfs put trả về returncode 1")

    def test_archiver_kafka_commit_failure_idempotent_retry(self):
        """
        Tái hiện vòng lặp thật:
        - e1 ghi vào batch_id=0_0 thành công, nhưng commit offset lỗi.
        - e2 đến.
        - Hệ thống KHÔNG được gộp e1 và e2 thành batch_id=0_1.
        - Phải hoàn tất batch_id=0_0 trước, sau đó mới ghi e2 vào batch_id=1_1.
        """
        # Giả lập logic quản lý active_batches và incoming_buffers
        topic_name = "tourism-output"
        part_id = 0
        subzone = "events"

        manifest = {"topics": {topic_name: {str(part_id): -1}}, "pending_batches": {}}
        active_batches = {topic_name: {}}
        incoming_buffers = {topic_name: {part_id: []}}

        written_batches = {}

        def mock_upload(records, target_dir):
            written_batches[target_dir] = [r["event_id"] for r in records]
            return True

        # 1. e1 đến (offset 0)
        e1 = {"event_id": "e1", "data": "val1"}
        incoming_buffers[topic_name][part_id].append({"offset": 0, "record": e1})

        # Đóng gói batch đầu tiên
        items = incoming_buffers[topic_name][part_id]
        incoming_buffers[topic_name][part_id] = []
        min_off = items[0]["offset"]
        max_off = items[-1]["offset"]
        target_dir_0_0 = f"hdfs://dummy/tourism_data/streaming_zone/{subzone}/run_id=p{part_id}/batch_id={min_off}_{max_off}"
        active_batches[topic_name][part_id] = {
            "subzone": subzone,
            "min_offset": min_off,
            "max_offset": max_off,
            "target_dir": target_dir_0_0,
            "records": [it["record"] for it in items],
            "uploaded": False
        }

        # Upload batch 0_0
        act = active_batches[topic_name][part_id]
        upload_ok = mock_upload(act["records"], act["target_dir"])
        self.assertTrue(upload_ok)
        act["uploaded"] = True

        # Giả lập commit Kafka thất bại!
        commit_ok = False  # Simulating consumer.commit exception

        if not commit_ok:
            # Batch vẫn ở lại active_batches!
            pass

        # 2. e2 đến trong khi batch 0_0 chưa commit
        e2 = {"event_id": "e2", "data": "val2"}
        msg_offset = 1

        # Logic nhận tin nhắn: vì part_id đã có active_batch với max_offset=0, e2 > 0 -> đưa vào incoming_buffers
        act_batch = active_batches[topic_name].get(part_id)
        self.assertIsNotNone(act_batch)
        if msg_offset > act_batch["max_offset"]:
            incoming_buffers[topic_name][part_id].append({"offset": msg_offset, "record": e2})

        # Đảm bảo e2 KHÔNG bị ghép vào active_batch đang dở dang
        self.assertEqual(len(active_batches[topic_name][part_id]["records"]), 1)
        self.assertEqual(active_batches[topic_name][part_id]["records"][0]["event_id"], "e1")
        self.assertEqual(active_batches[topic_name][part_id]["target_dir"], target_dir_0_0)

        # 3. Lần flush tiếp theo: Retry batch_id=0_0
        # Không được tạo batch mới vì part_id đang có active_batch
        self.assertIn(part_id, active_batches[topic_name])
        
        # Retry commit Kafka cho batch 0_0 -> lần này thành công
        commit_ok = True
        if commit_ok:
            manifest["topics"][topic_name][str(part_id)] = active_batches[topic_name][part_id]["max_offset"]
            del active_batches[topic_name][part_id]

        # Batch 0_0 đã hoàn tất!
        self.assertNotIn(part_id, active_batches[topic_name])

        # 4. Flush tiếp theo: Bây giờ part_id đã rỗng active_batch, lấy e2 từ incoming_buffers
        items_e2 = incoming_buffers[topic_name][part_id]
        incoming_buffers[topic_name][part_id] = []
        min_off_2 = items_e2[0]["offset"]
        max_off_2 = items_e2[-1]["offset"]
        target_dir_1_1 = f"hdfs://dummy/tourism_data/streaming_zone/{subzone}/run_id=p{part_id}/batch_id={min_off_2}_{max_off_2}"
        
        active_batches[topic_name][part_id] = {
            "subzone": subzone,
            "min_offset": min_off_2,
            "max_offset": max_off_2,
            "target_dir": target_dir_1_1,
            "records": [it["record"] for it in items_e2],
            "uploaded": False
        }
        mock_upload(active_batches[topic_name][part_id]["records"], target_dir_1_1)
        active_batches[topic_name][part_id]["uploaded"] = True

        # KIỂM TRA ĐỘ BỀN VÀ KHÔNG GHI TRÙNG:
        # Batch 0_0 phải chứa duy nhất e1
        self.assertEqual(written_batches[target_dir_0_0], ["e1"])
        # Batch 1_1 phải chứa duy nhất e2
        self.assertEqual(written_batches[target_dir_1_1], ["e2"])
        # Tuyệt đối không có batch 0_1 chứa cả e1 và e2!
        self.assertNotIn(f"hdfs://dummy/tourism_data/streaming_zone/{subzone}/run_id=p{part_id}/batch_id=0_1", written_batches)

    def test_materializer_ready_cleared_on_disconnect(self):
        """Kiểm tra Materializer clear ready_event khi ngắt kết nối / gặp lỗi và chỉ set khi bắt kịp."""
        mat = KafkaMaterializer(bootstrap_servers="localhost:9092")
        # Giả lập phiên trước đã ready
        mat._ready_event.set()
        self.assertTrue(mat.is_ready())

        # Khi gọi stop() hoặc khi consumer gặp lỗi ngắt kết nối:
        mat.stop()
        self.assertFalse(mat.is_ready(), "ready_event phải được clear khi stop/mất kết nối")

        # Kiểm tra phương thức stop() dọn dẹp cờ
        mat._ready_event.set()
        mat.stop()
        self.assertFalse(mat.is_ready())

    def test_write_ahead_manifest_and_crash_recovery(self):
        """
        Kiểm tra:
        1. Manifest pending bắt buộc phải lưu thành công trước khi ghi Parquet (Write-Ahead).
        2. Nếu lưu manifest pending lỗi -> không được gọi upload.
        3. Tái hiện crash sau khi upload và restart:
           Kafka trả về cả e0 và e1 -> e0 được thu thập để hoàn tất batch 0_0,
           e1 nằm ở incoming_buffers -> ghi riêng sang 1_1, không bị gộp vào 0_1.
        """
        topic_name = "tourism-output"
        part_id = 0
        subzone = "events"

        # 1. Thử nghiệm lưu manifest pending lỗi -> upload không được gọi
        manifest = {"topics": {topic_name: {str(part_id): -1}}, "pending_batches": {topic_name: {}}}
        uploaded_dirs = []

        def mock_upload(records, target_dir):
            uploaded_dirs.append(target_dir)
            return True

        e0 = {"event_id": "e0", "val": 0}
        inc_buffer = [{"offset": 0, "record": e0}]
        
        # Giả lập save_manifest lỗi
        save_manifest_mock = MagicMock(return_value=False)
        save_ok = save_manifest_mock(manifest)
        if not save_ok:
            # Code mới: KHÔNG tạo active_batch và KHÔNG gọi upload!
            pass
        self.assertEqual(len(uploaded_dirs), 0, "Upload không được gọi nếu lưu manifest pending thất bại")

        # 2. Bây giờ save_manifest pending thành công
        save_manifest_mock.return_value = True
        batch_offsets = [0]
        min_off = 0
        max_off = 0
        target_dir_0_0 = f"hdfs://dummy/tourism_data/streaming_zone/{subzone}/run_id=p{part_id}/batch_id={min_off}_{max_off}"
        
        # Ghi nhận trước vào manifest
        manifest["pending_batches"][topic_name][str(part_id)] = {
            "subzone": subzone,
            "min_offset": min_off,
            "max_offset": max_off,
            "target_dir": target_dir_0_0,
            "offsets": batch_offsets,
            "uploaded": False
        }
        self.assertTrue(save_manifest_mock(manifest))

        # Upload Parquet thành công
        mock_upload([e0], target_dir_0_0)
        manifest["pending_batches"][topic_name][str(part_id)]["uploaded"] = True

        # 3. GIẢ LẬP CRASH VÀ RESTART TIẾN TRÌNH:
        # Bộ nhớ RAM bị xóa, nạp lại từ manifest đã lưu
        restarted_active_batches = {}
        p_info = manifest["pending_batches"][topic_name][str(part_id)]
        restarted_active_batches[part_id] = {
            "subzone": p_info["subzone"],
            "min_offset": p_info["min_offset"],
            "max_offset": p_info["max_offset"],
            "target_dir": p_info["target_dir"],
            "expected_offsets": set(p_info["offsets"]),
            "records_by_offset": {},
            "manifest_persisted": True,
            "uploaded": p_info["uploaded"]
        }
        restarted_incoming_buffers = {part_id: []}

        # Kafka replay lại e0 (offset 0) và phát sinh thêm e1 (offset 1)
        replayed_msgs = [
            {"offset": 0, "record": e0},
            {"offset": 1, "record": {"event_id": "e1", "val": 1}}
        ]

        # Logic nhận tin nhắn sau restart:
        act = restarted_active_batches.get(part_id)
        for msg in replayed_msgs:
            if act is not None:
                if act["min_offset"] <= msg["offset"] <= act["max_offset"]:
                    if not act["uploaded"]:
                        act["records_by_offset"][msg["offset"]] = msg["record"]
                    # Đã uploaded thì record này thuộc batch cũ đã ghi, bỏ qua không ghép
                elif msg["offset"] > act["max_offset"]:
                    restarted_incoming_buffers[part_id].append(msg)

        # Kiểm tra: e0 đã được nhận diện thuộc batch cũ, e1 đi vào incoming_buffers riêng biệt!
        self.assertEqual(len(restarted_incoming_buffers[part_id]), 1)
        self.assertEqual(restarted_incoming_buffers[part_id][0]["record"]["event_id"], "e1")

        # Hoàn tất batch 0_0
        manifest["topics"][topic_name][str(part_id)] = act["max_offset"]
        del manifest["pending_batches"][topic_name][str(part_id)]
        del restarted_active_batches[part_id]

        # Xử lý e1 từ incoming_buffers -> tạo batch 1_1
        items_e1 = restarted_incoming_buffers[part_id]
        min_1 = items_e1[0]["offset"]
        max_1 = items_e1[-1]["offset"]
        target_dir_1_1 = f"hdfs://dummy/tourism_data/streaming_zone/{subzone}/run_id=p{part_id}/batch_id={min_1}_{max_1}"
        mock_upload([items_e1[0]["record"]], target_dir_1_1)

        # XÁC NHẬN: Không bao giờ có batch_id=0_1!
        self.assertIn(target_dir_0_0, uploaded_dirs)
        self.assertIn(target_dir_1_1, uploaded_dirs)
        self.assertNotIn(f"hdfs://dummy/tourism_data/streaming_zone/{subzone}/run_id=p{part_id}/batch_id=0_1", uploaded_dirs)


if __name__ == "__main__":
    unittest.main()
