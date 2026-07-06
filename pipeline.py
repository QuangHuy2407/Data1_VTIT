"""
pipeline.py  —  Prefect Pipeline
=================================
Flow tự động 3 task:

  Task 1 — generate_and_encrypt_task()   gọi generate_and_encrypt_pipeline()
  Task 2 — upload_to_minio_task()        gọi upload_to_minio()
  Task 3 — insert_to_clickhouse_task()   chạy insertData.py qua runpy

Cách chạy:
    python pipeline.py
    python pipeline.py --rows 500

Xem UI Prefect (tuỳ chọn):
    prefect server start        # mở http://localhost:4200
"""

import sys
import runpy
import argparse
from pathlib import Path

# ── Thêm thư mục vào sys.path ─────────────────────────────────────────────────
ROOT_DIR = Path(__file__).parent
sys.path.insert(0, str(ROOT_DIR / "ingestion"))
sys.path.insert(0, str(ROOT_DIR / "analytics_database"))

# ── Prefect ───────────────────────────────────────────────────────────────────
from prefect import flow, task, get_run_logger

# ── Import hàm từ các file gốc ────────────────────────────────────────────────
from mock_data_encryption import generate_and_encrypt_pipeline
from upLoadFile import upload_to_minio, BUCKET_NAME, MINIO_OBJECT_NAME


# ══════════════════════════════════════════════════════════════════════════════
# TASK 1: Sinh dữ liệu & mã hóa AES-256-GCM
# ══════════════════════════════════════════════════════════════════════════════
@task(name="Sinh dữ liệu & mã hóa AES-256-GCM", retries=2, retry_delay_seconds=10)
def generate_and_encrypt_task(num_rows: int, parquet_path: str) -> str:
    logger = get_run_logger()
    logger.info(f"Đang sinh {num_rows:,} dòng dữ liệu và mã hóa...")

    df = generate_and_encrypt_pipeline(num_rows=num_rows)
    df.write_parquet(parquet_path)

    logger.info(f"✅ Đã lưu {num_rows:,} dòng → {parquet_path}")
    return parquet_path


# ══════════════════════════════════════════════════════════════════════════════
# TASK 2: Upload file Parquet lên MinIO
# ══════════════════════════════════════════════════════════════════════════════
@task(name="Upload file lên MinIO", retries=3, retry_delay_seconds=15)
def upload_to_minio_task(parquet_path: str):
    logger = get_run_logger()
    logger.info(f"Đang upload {parquet_path} → MinIO ({BUCKET_NAME}/{MINIO_OBJECT_NAME})...")

    upload_to_minio(parquet_path, BUCKET_NAME, MINIO_OBJECT_NAME)

    logger.info("✅ Upload MinIO thành công!")


# ══════════════════════════════════════════════════════════════════════════════
# TASK 3: Nạp dữ liệu từ MinIO vào ClickHouse & di chuyển file
# ══════════════════════════════════════════════════════════════════════════════
@task(name="Nạp dữ liệu vào ClickHouse", retries=2, retry_delay_seconds=20)
def insert_to_clickhouse_task():
    logger = get_run_logger()
    logger.info("Đang nạp dữ liệu từ MinIO vào ClickHouse...")

    runpy.run_path(str(ROOT_DIR / "analytics_database" / "insertData.py"))

    logger.info("✅ Nạp ClickHouse & di chuyển file hoàn tất!")


# ══════════════════════════════════════════════════════════════════════════════
# FLOW CHÍNH
# ══════════════════════════════════════════════════════════════════════════════
@flow(name="Data Pipeline: Encrypt → MinIO → ClickHouse", log_prints=True)
def run_pipeline(num_rows: int = 100):
    parquet_path = str(ROOT_DIR / "ingestion" / "encrypted_customer_data_1M.parquet")

    # Task chạy tuần tự — kết quả task trước làm đầu vào task sau
    saved_path = generate_and_encrypt_task(num_rows=num_rows, parquet_path=parquet_path)
    upload_to_minio_task(parquet_path=saved_path)
    insert_to_clickhouse_task()


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Prefect Data Pipeline tự động")
    parser.add_argument("--rows", type=int, default=100,
                        help="Số dòng dữ liệu giả lập (mặc định: 100)")
    args = parser.parse_args()
    run_pipeline(num_rows=args.rows)
