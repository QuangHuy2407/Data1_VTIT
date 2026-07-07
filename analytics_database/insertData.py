import os
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv
import clickhouse_connect
import boto3
from botocore.client import Config

# 1. Load các biến môi trường từ file .env
_env_path = Path(__file__).parent / ".env"
load_dotenv(dotenv_path=_env_path)
#print(f"[INFO] Loading .env from: {_env_path}")

# Lấy cấu hình kết nối MinIO
MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT")
ACCESS_KEY       = os.getenv("ACCESS_KEY")
SECRET_KEY       = os.getenv("SECRET_KEY")
BUCKET_NAME      = os.getenv("BUCKET_NAME")

# Lấy cấu hình kết nối ClickHouse
CLICKHOUSE_HOST     = "localhost" 
CLICKHOUSE_PORT     = int(os.getenv("CLICKHOUSE_PORT"))
CLICKHOUSE_USER     = os.getenv("CLICKHOUSE_USER")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD")
CLICKHOUSE_DB       = os.getenv("CLICKHOUSE_DB")

# --- ĐỌC CẤU HÌNH FILE ĐỘNG TỪ .ENV (KHÔNG GHI ĐÈ VÀO CODE) ---
SOURCE_KEY        = os.getenv("MINIO_SOURCE_KEY")
TARGET_FOLDER     = os.getenv("MINIO_TARGET_FOLDER")
TARGET_PREFIX     = os.getenv("MINIO_TARGET_FILE_PREFIX")
CLICKHOUSE_S3_URL = os.getenv("MINIO_BUCKET_URL_UPLOAD")

# Tự động tạo tên file đích kèm timestamp bảo toàn dữ liệu cũ
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
TARGET_KEY = f"{TARGET_FOLDER}/{TARGET_PREFIX}_{timestamp}.parquet"

# Kiểm tra an toàn biến môi trường
_missing = [k for k, v in {
    "MINIO_ENDPOINT": MINIO_ENDPOINT, 
    "SOURCE_KEY": SOURCE_KEY, 
    "TARGET_FOLDER": TARGET_FOLDER,
    "CLICKHOUSE_S3_URL": CLICKHOUSE_S3_URL
}.items() if not v]

if _missing:
    raise EnvironmentError(f"[ERROR] Thiếu cấu hình ẩn trong .env: {', '.join(_missing)}")

# 2. Khởi tạo kết nối tới ClickHouse
client = clickhouse_connect.get_client(
    host=CLICKHOUSE_HOST,
    port=CLICKHOUSE_PORT,
    username=CLICKHOUSE_USER,
    password=CLICKHOUSE_PASSWORD,
    database=CLICKHOUSE_DB
)

# Khởi tạo kết nối tới MinIO bằng Boto3
s3_client = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    config=Config(signature_version='s3v4')
)

# 3. Câu lệnh SQL nạp dữ liệu
query = f"""
INSERT INTO customer_data
SELECT * FROM s3(
    '{CLICKHOUSE_S3_URL}',
    '{ACCESS_KEY}',
    '{SECRET_KEY}',
    'Parquet'
);
"""

# 4. Thực thi nạp và di chuyển file
try:
    #print(f"[INFO] Đang nạp dữ liệu từ vị trí cấu hình bí mật vào ClickHouse...")
    client.command(query)
    #print("[OK] Đã nạp dữ liệu vào ClickHouse thành công!")
    
    # --- TIẾN HÀNH DI CHUYỂN FILE SANG USED DATA ---
    #print(f"[INFO] Đang di chuyển bảo tồn file theo cấu hình hệ thống...")
    
    # Bước 4.1: Sao chép (Copy)
    s3_client.copy_object(
        Bucket=BUCKET_NAME,
        CopySource={'Bucket': BUCKET_NAME, 'Key': SOURCE_KEY},
        Key=TARGET_KEY
    )
    
    # Bước 4.2: Xóa (Delete) file cũ
    s3_client.delete_object(
        Bucket=BUCKET_NAME,
        Key=SOURCE_KEY
    )
    
    #print(f"[OK] Di chuyển hoàn tất! File đã được lưu trữ an toàn.")

except Exception as e:
    print(f"[ERROR] Quá trình xử lý thất bại: {e}")
    
finally:
    client.close()