"""
TIỆN ÍCH KHỞI TẠO VÀ XÁC THỰC KAFKA TOPICS CHO HỆ THỐNG LAMBDA ARCHITECTURE
=============================================================================
Đảm bảo tất cả 6 topics cốt lõi được cấu hình chính xác trước khi pipeline chạy:
- tourism-input (3 partitions, delete)
- tourism-output (3 partitions, delete)
- tourism-reviews (3 partitions, compact, retention.ms=-1)
- tourism-state-spots (1 partition, compact, max.message.bytes=10MB)
- tourism-state-batch (1 partition, compact, max.message.bytes=10MB)
- tourism-control (1 partition, compact)
- tourism-dead-letter (1 partition, delete)
=============================================================================
"""

import os
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

if sys.platform.startswith('win'):
    try:
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
        sys.stderr.reconfigure(encoding='utf-8', errors='replace')
    except Exception:
        pass

from messaging.state_store import (
    ensure_topics,
    TOPIC_SPECS,
    DEFAULT_BOOTSTRAP_SERVERS
)


def ensure_all_topics(bootstrap_servers=None) -> bool:
    """
    Tạo và kiểm tra hợp chuẩn tất cả các topic Kafka theo yêu cầu Lambda Architecture.
    Trả về True nếu thành công, False nếu không thể kết nối hoặc gặp lỗi.
    """
    servers = bootstrap_servers or DEFAULT_BOOTSTRAP_SERVERS
    print("=" * 75)
    print("  KHỞI TẠO VÀ XÁC THỰC KAFKA TOPICS (KAFKA-FIRST LAMBDA ARCHITECTURE)")
    print(f"  Bootstrap Servers: {servers}")
    print("=" * 75)

    try:
        ensure_topics(bootstrap_servers=servers)
        print("[+] Xác thực và cấu hình toàn bộ Kafka topics thành công:")
        for topic_name, spec in TOPIC_SPECS.items():
            print(f"    - {topic_name:24} : {spec['partitions']} partition(s), policy='{spec['cleanup.policy']}'")
        return True
    except Exception as e:
        print(f"[!] Lỗi khi kết nối hoặc kiểm tra cấu hình topics: {e}")
        return False


if __name__ == '__main__':
    ok = ensure_all_topics()
    sys.exit(0 if ok else 1)
