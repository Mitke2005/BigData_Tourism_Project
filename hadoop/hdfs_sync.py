"""
MODULE ĐỒNG BỘ & ĐẨY DATASET LÊN HDFS THẬT (HADOOP CLUSTER)
Xác nhận 100% tệp đã nằm trên HDFS hdfs://localhost:9000 trước khi Spark đọc.
Hỗ trợ chạy trên Windows với Hadoop cài trên WSL.
"""

import os
import sys
import time
import socket
import subprocess

if sys.platform.startswith('win'):
    try:
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
        sys.stderr.reconfigure(encoding='utf-8', errors='replace')
    except:
        pass

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")
CLEAN_CSV = os.path.join(DATA_DIR, "tourism_dataset_clean.csv")

HDFS_URI = "hdfs://localhost:9000/tourism_data/processed_zone/tourism_dataset_clean.csv"

# --- Cấu hình WSL Hadoop ---
WSL_HADOOP_HOME = "/home/mitke/hadoop"
WSL_JAVA_HOME = "/usr/lib/jvm/java-8-openjdk-amd64"


def _use_wsl():
    """Kiểm tra xem có nên dùng WSL để chạy lệnh Hadoop hay không."""
    if not sys.platform.startswith('win'):
        return False
    # Trên Windows, kiểm tra xem Hadoop có cài trên WSL không
    hadoop_home = os.environ.get("HADOOP_HOME", "")
    hdfs_cmd = os.path.join(hadoop_home, "bin", "hdfs.cmd") if hadoop_home else ""
    if hadoop_home and os.path.exists(hdfs_cmd):
        return False  # Hadoop cài trên Windows trực tiếp
    # Kiểm tra WSL có sẵn không
    try:
        r = subprocess.run(["wsl", "-e", "test", "-f", f"{WSL_HADOOP_HOME}/bin/hdfs"],
                           capture_output=True, timeout=5)
        return r.returncode == 0
    except Exception:
        return False


def _run_wsl_hdfs(*args, check=False, capture_output=False, text=False, **kwargs):
    """Chạy lệnh hdfs thông qua WSL."""
    hdfs_args = " ".join(args)
    bash_cmd = f"export JAVA_HOME={WSL_JAVA_HOME} && export HADOOP_HOME={WSL_HADOOP_HOME} && {WSL_HADOOP_HOME}/bin/hdfs {hdfs_args}"
    cmd = ["wsl", "-e", "bash", "-c", bash_cmd]
    return subprocess.run(cmd, check=check, capture_output=capture_output, text=text, **kwargs)


def _win_to_wsl_path(win_path):
    """Chuyển đổi đường dẫn Windows sang đường dẫn WSL."""
    try:
        r = subprocess.run(["wsl", "-e", "wslpath", "-a", win_path.replace('\\', '/')],
                           capture_output=True, text=True, timeout=5)
        if r.returncode == 0 and r.stdout.strip():
            return r.stdout.strip()
    except Exception:
        pass
    # Fallback: chuyển đổi thủ công C:\... -> /mnt/c/...
    path = win_path.replace('\\', '/')
    if len(path) >= 2 and path[1] == ':':
        drive = path[0].lower()
        path = f"/mnt/{drive}{path[2:]}"
    return path


USE_WSL = None  # Lazy init


def _should_use_wsl():
    global USE_WSL
    if USE_WSL is None:
        USE_WSL = _use_wsl()
        if USE_WSL:
            print("[*] Phát hiện Hadoop trên WSL, sẽ dùng WSL để thao tác HDFS.")
    return USE_WSL


def is_hdfs_port_open():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(2.0)
    try:
        s.connect(('localhost', 9000))
        s.close()
        return True
    except Exception:
        return False

def ensure_hdfs_services():
    """Đảm bảo dịch vụ Hadoop NameNode & DataNode đang chạy."""
    if is_hdfs_port_open():
        print("[+] Hadoop HDFS NameNode đang hoạt động tại hdfs://localhost:9000.")
        return True

    print("[*] Đang khởi động dịch vụ Hadoop NameNode & DataNode...")

    if _should_use_wsl():
        # Khởi động qua WSL
        bash_cmd = (
            f"export JAVA_HOME={WSL_JAVA_HOME} && "
            f"export HADOOP_HOME={WSL_HADOOP_HOME} && "
            f"{WSL_HADOOP_HOME}/bin/hdfs --daemon start namenode 2>&1 && "
            f"{WSL_HADOOP_HOME}/bin/hdfs --daemon start datanode 2>&1"
        )
        subprocess.run(["wsl", "-e", "bash", "-c", bash_cmd],
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    else:
        default_hadoop = "C:/hadoop" if sys.platform.startswith('win') else (
            "/home/mitke/hadoop" if os.path.exists("/home/mitke/hadoop") else "/usr/local/hadoop"
        )
        hadoop_home = os.environ.get("HADOOP_HOME", default_hadoop)
        hadoop_home = os.path.abspath(hadoop_home).replace('\\', '/')
        default_java = "C:/jdk-17" if sys.platform.startswith('win') else (
            "/usr/lib/jvm/java-8-openjdk-amd64" if os.path.exists("/usr/lib/jvm/java-8-openjdk-amd64") else "/usr/lib/jvm/java-17-openjdk-amd64"
        )
        java_home = os.environ.get("JAVA_HOME", default_java)
        env = os.environ.copy()
        env["HADOOP_HOME"] = hadoop_home
        env["JAVA_HOME"] = java_home

        hdfs_cmd_name = "hdfs.cmd" if sys.platform.startswith('win') else "hdfs"
        hdfs_cmd = os.path.join(hadoop_home, "bin", hdfs_cmd_name)
        if not os.path.exists(hdfs_cmd):
            hdfs_cmd = hdfs_cmd_name
        if not sys.platform.startswith('win') and os.path.exists(hdfs_cmd):
            subprocess.run([hdfs_cmd, "--daemon", "start", "namenode"], env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            subprocess.run([hdfs_cmd, "--daemon", "start", "datanode"], env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        else:
            flags = (subprocess.DETACHED_PROCESS | subprocess.CREATE_NEW_PROCESS_GROUP) if sys.platform.startswith('win') else 0
            if os.path.exists(hdfs_cmd) or not sys.platform.startswith('win'):
                subprocess.Popen([hdfs_cmd, "namenode"], env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, creationflags=flags)
                subprocess.Popen([hdfs_cmd, "datanode"], env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, creationflags=flags)

    # Chờ NameNode mở cổng 9000 (tối đa 25 giây)
    for _ in range(25):
        time.sleep(1.0)
        if is_hdfs_port_open():
            print("[+] Khởi động Hadoop HDFS thành công trên cổng 9000!")
            return True

    if is_hdfs_port_open():
        print("[+] Khởi động Hadoop HDFS thành công trên cổng 9000!")
        return True
    else:
        raise ConnectionError("Không thể kết nối cụm HDFS cổng 9000. Vui lòng kiểm tra daemon NameNode.")

def upload_dataset_to_hdfs():
    """
    Thực hiện upload tệp dữ liệu sạch lên HDFS và xác nhận thành công.
    KHÔNG DÙNG FALLBACK. Bắt buộc kiểm chứng bằng lệnh hdfs dfs -ls / -test.
    Hỗ trợ Hadoop trên WSL khi chạy từ Windows.
    """
    print("="*75)
    print("  BƯỚC 1: ĐƯA DATASET LÊN HDFS THẬT (HADOOP CLUSTER hdfs://localhost:9000)")
    print("="*75)

    ensure_hdfs_services()

    if _should_use_wsl():
        # === Chế độ WSL ===
        # 1. Tạo thư mục trên HDFS
        _run_wsl_hdfs("dfs", "-mkdir", "-p", "/tourism_data/processed_zone", "/tourism_data/raw_zone",
                      check=True)

        # 2. Upload tệp lên HDFS (chuyển đường dẫn Windows -> WSL)
        wsl_csv_path = _win_to_wsl_path(CLEAN_CSV)
        print(f"[*] Đang thực thi qua WSL: hdfs dfs -put -f {wsl_csv_path} -> /tourism_data/processed_zone/...")
        _run_wsl_hdfs("dfs", "-put", "-f", wsl_csv_path,
                      "/tourism_data/processed_zone/tourism_dataset_clean.csv", check=True)

        # 3. Kiểm chứng tệp trên HDFS
        res_test = _run_wsl_hdfs("dfs", "-test", "-e",
                                 "/tourism_data/processed_zone/tourism_dataset_clean.csv")
        if res_test.returncode != 0:
            raise FileNotFoundError("Kiểm chứng HDFS thất bại: Tệp không tồn tại sau khi upload.")

        # 4. Hiển thị thông tin tệp trên HDFS
        p_ls = _run_wsl_hdfs("dfs", "-ls", "/tourism_data/processed_zone/",
                             capture_output=True, text=True)
        print("[+] [XÁC NHẬN HDFS] Tệp dữ liệu đã nằm trên cụm HDFS thật:")
        print(p_ls.stdout.strip())
    else:
        # === Chế độ Windows / Linux gốc ===
        default_hadoop = "C:/hadoop" if sys.platform.startswith('win') else (
            "/home/mitke/hadoop" if os.path.exists("/home/mitke/hadoop") else "/usr/local/hadoop"
        )
        hadoop_home = os.environ.get("HADOOP_HOME", default_hadoop)
        hadoop_home = os.path.abspath(hadoop_home).replace('\\', '/')
        default_java = "C:/jdk-17" if sys.platform.startswith('win') else (
            "/usr/lib/jvm/java-8-openjdk-amd64" if os.path.exists("/usr/lib/jvm/java-8-openjdk-amd64") else "/usr/lib/jvm/java-17-openjdk-amd64"
        )
        java_home = os.environ.get("JAVA_HOME", default_java)
        env = os.environ.copy()
        env["HADOOP_HOME"] = hadoop_home
        env["JAVA_HOME"] = java_home
        hdfs_cmd_name = "hdfs.cmd" if sys.platform.startswith('win') else "hdfs"
        hdfs_cmd = os.path.join(hadoop_home, "bin", hdfs_cmd_name)
        if not os.path.exists(hdfs_cmd):
            hdfs_cmd = hdfs_cmd_name

        # 1. Tạo thư mục trên HDFS
        cmd_mkdir = [hdfs_cmd, "dfs", "-mkdir", "-p", "/tourism_data/processed_zone", "/tourism_data/raw_zone"]
        subprocess.run(cmd_mkdir, env=env, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

        # 2. Upload tệp lên HDFS
        clean_csv_clean = CLEAN_CSV.replace('\\', '/')
        cmd_put = [hdfs_cmd, "dfs", "-put", "-f", clean_csv_clean, "/tourism_data/processed_zone/tourism_dataset_clean.csv"]
        print(f"[*] Đang thực thi: hdfs dfs -put -f {clean_csv_clean} -> /tourism_data/processed_zone/...")
        subprocess.run(cmd_put, env=env, check=True)

        # 3. Kiểm chứng sự tồn tại của tệp trên HDFS bằng lệnh hdfs dfs -test
        cmd_test = [hdfs_cmd, "dfs", "-test", "-e", "/tourism_data/processed_zone/tourism_dataset_clean.csv"]
        res_test = subprocess.run(cmd_test, env=env)
        if res_test.returncode != 0:
            raise FileNotFoundError("Kiểm chứng HDFS thất bại: Tệp không tồn tại sau khi upload.")

        # 4. Hiển thị thông tin tệp trên HDFS
        cmd_ls = [hdfs_cmd, "dfs", "-ls", "/tourism_data/processed_zone/"]
        p_ls = subprocess.run(cmd_ls, env=env, capture_output=True, text=True)
        print("[+] [XÁC NHẬN HDFS] Tệp dữ liệu đã nằm trên cụm HDFS thật:")
        print(p_ls.stdout.strip())

    print(f"[+] Đường dẫn HDFS sẵn sàng cho Apache Spark: {HDFS_URI}")
    print("="*75)
    return HDFS_URI

if __name__ == '__main__':
    upload_dataset_to_hdfs()
