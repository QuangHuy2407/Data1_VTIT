import os
import boto3
from pathlib import Path
from botocore.exceptions import NoCredentialsError
from dotenv import load_dotenv

# 1. Load biến môi trường từ file .env (cùng thư mục với script)
# Dùng đường dẫn tuyệt đối để tránh lỗi khi chạy từ thư mục khác
_env_path = Path(__file__).parent / ".env"
load_dotenv(dotenv_path=_env_path)
print(f"[INFO] Loading .env from: {_env_path}")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
ACCESS_KEY     = os.getenv("ACCESS_KEY")
SECRET_KEY     = os.getenv("SECRET_KEY")

# Bắt buộc phải có đủ 3 biến – không dùng giá trị mặc định để tránh lộ credentials
_missing = [k for k, v in {"MINIO_ENDPOINT": MINIO_ENDPOINT, "ACCESS_KEY": ACCESS_KEY, "SECRET_KEY": SECRET_KEY}.items() if not v]
if _missing:
    raise EnvironmentError(f"Thiếu biến môi trường trong .env: {', '.join(_missing)}")

BUCKET_NAME       = "data-pipeline"
# Duong dan tuyet doi toi file parquet (relative to script directory's parent)
LOCAL_FILE_PATH   = str(Path(__file__).parent.parent / "ingestion" / "encrypted_customer_data_1M.parquet")
MINIO_OBJECT_NAME = "datasets/NewData/data.parquet"

# 2. Khởi tạo client kết nối với S3 API của MinIO
s3_client = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
)

def upload_to_minio(local_file, bucket, object_name):
    try:
        # Kiểm tra và tạo bucket nếu chưa tồn tại (tùy chọn)
        # s3_client.create_bucket(Bucket=bucket)

        # Upload file
        print(f"[INFO] Uploading: {local_file}")
        s3_client.upload_file(local_file, bucket, object_name)
        print(f"[OK] Upload success: {bucket}/{object_name}")
    except FileNotFoundError:
        print(f"[ERROR] Local file not found: {local_file}")
    except NoCredentialsError:
        print("[ERROR] Invalid credentials.")
    except Exception as e:
        print(f"[ERROR] {e}")

# Chạy hàm upload
upload_to_minio(LOCAL_FILE_PATH, BUCKET_NAME, MINIO_OBJECT_NAME)
