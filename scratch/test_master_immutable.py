import os
import sys
import hashlib
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


class TestMasterDatasetImmutable(unittest.TestCase):

    def test_master_dataset_sha256(self):
        csv_path = os.path.join(BASE_DIR, "data", "tourism_dataset_clean.csv")
        hash_file = os.path.join(BASE_DIR, "data", "MASTER_DATASET.sha256")

        self.assertTrue(os.path.exists(csv_path), "File Master CSV không tồn tại!")
        self.assertTrue(os.path.exists(hash_file), "File sha256 khóa không tồn tại!")

        with open(hash_file, "r", encoding="utf-8") as f:
            expected_hash = f.read().strip().lower()

        sha256_func = hashlib.sha256()
        with open(csv_path, "rb") as f:
            for chunk in iter(lambda: f.read(65536), b""):
                sha256_func.update(chunk)
        actual_hash = sha256_func.hexdigest().lower()

        self.assertEqual(
            actual_hash,
            expected_hash,
            f"BẢO TOÀN THẤT BẠI: Master CSV đã bị thay đổi! Actual: {actual_hash}, Expected: {expected_hash}"
        )
        print(f"[+] [Test Master Immutable] SHA-256 khớp 100%: {actual_hash}")


if __name__ == '__main__':
    unittest.main()
