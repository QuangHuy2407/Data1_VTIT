import boto3
from botocore.exceptions import NoCredentialsError
from minio_credentials import MINIO_ENDPOINT, ACCESS_KEY, SECRET_KEY

# 1. Cấu hình thông tin kết nối MinIO
MINIO_ENDPOINT = MINIO_ENDPOINT  # Thay bằng IP/Domain MinIO của bạn
ACCESS_KEY = ACCESS_KEY
SECRET_KEY = SECRET_KEY

BUCKET_NAME = "data-pipeline"
LOCAL_FILE_PATH = "encrypted_customer_data.parquet"          # Đường dẫn file ở máy bạn
MINIO_OBJECT_NAME = "datasets/data.parquet" # Đường dẫn lưu trên MinIO

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
        print(f" Đã đẩy file thành công lên: {bucket}/{object_name}")
    except FileNotFoundError:
        print(" Tệp tin cục bộ không tồn tại.")
    except NoCredentialsError:
        print(" Sai thông tin credentials.")
    except Exception as e:
        print(f" Có lỗi xảy ra: {e}")

# Chạy hàm upload
upload_to_minio(LOCAL_FILE_PATH, BUCKET_NAME, MINIO_OBJECT_NAME)