import os
import boto3
from botocore.exceptions import NoCredentialsError
from dotenv import load_dotenv

# 1. Load biến môi trường từ file .env (cùng thư mục với script)
load_dotenv()

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
ACCESS_KEY     = os.getenv("ACCESS_KEY")
SECRET_KEY     = os.getenv("SECRET_KEY")

# Bắt buộc phải có đủ 3 biến – không dùng giá trị mặc định để tránh lộ credentials
_missing = [k for k, v in {"MINIO_ENDPOINT": MINIO_ENDPOINT, "ACCESS_KEY": ACCESS_KEY, "SECRET_KEY": SECRET_KEY}.items() if not v]
if _missing:
    raise EnvironmentError(f"Thiếu biến môi trường trong .env: {', '.join(_missing)}")

BUCKET_NAME      = "data-pipeline"
LOCAL_FILE_PATH  = "encrypted_customer_data.parquet"  # Đường dẫn file ở máy bạn
MINIO_OBJECT_NAME = "datasets/data.parquet"           # Đường dẫn lưu trên MinIO

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

        # Đẩy file lên
        s3_client.upload_file(local_file, bucket, object_name)
        print(f"Đã đẩy file thành công lên: {bucket}/{object_name}")
    except FileNotFoundError:
        print("Tệp tin cục bộ không tồn tại.")
    except NoCredentialsError:
        print("Sai thông tin credentials.")
    except Exception as e:
        print(f"Có lỗi xảy ra: {e}")

# Chạy hàm upload
upload_to_minio(LOCAL_FILE_PATH, BUCKET_NAME, MINIO_OBJECT_NAME)
