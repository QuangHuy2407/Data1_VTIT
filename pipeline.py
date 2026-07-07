"""
Xem UI Prefect (tuỳ chọn):
    prefect server start        # mở http://localhost:4200
"""

import sys
import runpy
import argparse
from pathlib import Path

# ── Thêm thư mục vào sys.path ─────────────────────────────────────────────────
ROOT_DIR = Path(__file__).parent
DATA_DIR = ROOT_DIR / "Data"
DATA_DIR.mkdir(parents=True, exist_ok=True)
sys.path.insert(0, str(ROOT_DIR / "ingestion"))
sys.path.insert(0, str(ROOT_DIR / "analytics_database"))

# ── Prefect ───────────────────────────────────────────────────────────────────
from prefect import flow, task, get_run_logger

# ── Import hàm từ các file gốc ────────────────────────────────────────────────
from ingestion.mock_data_encryption import generate_and_encrypt_pipeline
from analytics_database.upLoadFile import upload_to_minio, BUCKET_NAME, MINIO_OBJECT_NAME


# ══════════════════════════════════════════════════════════════════════════════
# TASK 1: Sinh dữ liệu & mã hóa AES-256-GCM
# ══════════════════════════════════════════════════════════════════════════════
@task(name="Generate Data & AES-256-GCM Encryption", retries=2, retry_delay_seconds=10)
def generate_and_encrypt_task(num_rows: int, parquet_path: str) -> str:
    logger = get_run_logger()
    logger.info(f"[Task 1] Bắt đầu sinh {num_rows:,} dòng dữ liệu và mã hóa AES-256-GCM...")

    try:
        logger.info("[Task 1] Đang gọi generate_and_encrypt_pipeline()...")
        df = generate_and_encrypt_pipeline(num_rows=num_rows)
        logger.info(f"[Task 1] Sinh dữ liệu thành công — {num_rows:,} dòng. Đang ghi Parquet → {parquet_path}")

        df.write_parquet(parquet_path)
        logger.info(f"[Task 1] ✅ Đã lưu {num_rows:,} dòng → {parquet_path}")
        return parquet_path

    except Exception as e:
        logger.error(f"[Task 1] ❌ Lỗi khi sinh/mã hóa dữ liệu: {type(e).__name__}: {e}")
        logger.exception("[Task 1] Chi tiết traceback:")
        raise


# ══════════════════════════════════════════════════════════════════════════════
# TASK 2: Upload file Parquet lên MinIO
# ══════════════════════════════════════════════════════════════════════════════
@task(name="Upload Parquet File to MinIO", retries=3, retry_delay_seconds=15)
def upload_to_minio_task(parquet_path: str):
    logger = get_run_logger()
    logger.info(f"[Task 2] Bắt đầu upload {parquet_path} → MinIO ({BUCKET_NAME}/{MINIO_OBJECT_NAME})...")

    try:
        # Kiểm tra file tồn tại trước khi upload
        if not Path(parquet_path).exists():
            raise FileNotFoundError(f"Không tìm thấy file Parquet tại: {parquet_path}")

        logger.info(f"[Task 2] File tồn tại ({Path(parquet_path).stat().st_size / 1024:.1f} KB). Đang upload...")
        upload_to_minio(parquet_path, BUCKET_NAME, MINIO_OBJECT_NAME)
        logger.info(f"[Task 2] ✅ Upload MinIO thành công! Bucket: {BUCKET_NAME}, Object: {MINIO_OBJECT_NAME}")

    except FileNotFoundError as e:
        logger.error(f"[Task 2] ❌ File không tồn tại: {e}")
        logger.exception("[Task 2] Chi tiết traceback:")
        raise
    except Exception as e:
        logger.error(f"[Task 2] ❌ Lỗi khi upload lên MinIO: {type(e).__name__}: {e}")
        logger.exception("[Task 2] Chi tiết traceback:")
        raise


# ══════════════════════════════════════════════════════════════════════════════
# TASK 3: Nạp dữ liệu từ MinIO vào ClickHouse & di chuyển file
# ══════════════════════════════════════════════════════════════════════════════
@task(name="Load Data into ClickHouse", retries=2, retry_delay_seconds=20)
def insert_to_clickhouse_task():
    logger = get_run_logger()
    insert_script = ROOT_DIR / "analytics_database" / "insertData.py"
    logger.info(f"[Task 3] Bắt đầu nạp dữ liệu từ MinIO vào ClickHouse (script: {insert_script})...")

    try:
        # Kiểm tra script tồn tại
        if not insert_script.exists():
            raise FileNotFoundError(f"Không tìm thấy script: {insert_script}")

        logger.info("[Task 3] Đang chạy insertData.py qua runpy...")
        runpy.run_path(str(insert_script))
        logger.info("[Task 3] ✅ Nạp ClickHouse & di chuyển file hoàn tất!")

    except FileNotFoundError as e:
        logger.error(f"[Task 3] ❌ Script không tồn tại: {e}")
        logger.exception("[Task 3] Chi tiết traceback:")
        raise
    except Exception as e:
        logger.error(f"[Task 3] ❌ Lỗi khi nạp dữ liệu vào ClickHouse: {type(e).__name__}: {e}")
        logger.exception("[Task 3] Chi tiết traceback:")
        raise


# ══════════════════════════════════════════════════════════════════════════════
# FLOW CHÍNH
# ══════════════════════════════════════════════════════════════════════════════
@flow(name="Data Pipeline: Encrypt → MinIO → ClickHouse", log_prints=True)
def run_pipeline(num_rows: int = 100):
    logger = get_run_logger()
    parquet_path = str(DATA_DIR / "Data_mock.parquet")

    logger.info("=" * 60)
    logger.info(f"🚀 Khởi động pipeline với {num_rows:,} dòng dữ liệu")
    logger.info(f"   Parquet output: {parquet_path}")
    logger.info("=" * 60)

    try:
        # ── Task 1 ──────────────────────────────────────────────────────────
        logger.info("[Flow] ▶ Chạy Task 1: Sinh dữ liệu & mã hóa...")
        saved_path = generate_and_encrypt_task(num_rows=num_rows, parquet_path=parquet_path)
        logger.info("[Flow] Task 1 hoàn tất ✓")

        # ── Task 2 ──────────────────────────────────────────────────────────
        logger.info("[Flow] ▶ Chạy Task 2: Upload lên MinIO...")
        upload_to_minio_task(parquet_path=saved_path)
        logger.info("[Flow] Task 2 hoàn tất ✓")

        # ── Task 3 ──────────────────────────────────────────────────────────
        logger.info("[Flow] ▶ Chạy Task 3: Nạp vào ClickHouse...")
        insert_to_clickhouse_task()
        logger.info("[Flow] Task 3 hoàn tất ✓")

        logger.info("=" * 60)
        logger.info("🎉 Pipeline hoàn thành thành công!")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"[Flow] 💥 Pipeline thất bại tại bước: {type(e).__name__}: {e}")
        logger.error("[Flow] Kiểm tra log của từng Task bên trên để xác định nguyên nhân.")
        raise


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Prefect Data Pipeline tự động")
    parser.add_argument("--rows", type=int, default=100000,
                        help="Số dòng dữ liệu giả lập (mặc định: 100000)")
    args = parser.parse_args()
    run_pipeline(num_rows=args.rows)
