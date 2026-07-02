import subprocess
import sys
import os

# Thư mục chứa các file pipeline (cùng thư mục với file này)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

SCRIPTS = [
    ("BƯỚC 1 — Ingestion (pipeline_ingestion.py)", os.path.join(BASE_DIR, "pipeline_ingestion.py")),
    ("BƯỚC 2 — Upload    (upLoadFile.py)",          os.path.join(BASE_DIR, "upLoadFile.py")),
]

if __name__ == "__main__":
    print("\n" + "=" * 60)
    print("  PIPELINE TONG HOP BAT DAU")
    print("=" * 60)

    for label, script_path in SCRIPTS:
        print(f"\n{'-' * 60}")
        print(f"  {label}")
        print(f"{'-' * 60}\n")

        result = subprocess.run(
            [sys.executable, script_path],
            cwd=BASE_DIR,           # chay tu thu muc goc de duong dan file dung
        )

        if result.returncode != 0:
            print(f"\nPIPELINE DUNG: '{script_path}' ket thuc voi loi (exit code {result.returncode}).")
            sys.exit(result.returncode)

    print("\n" + "=" * 60)
    print("  PIPELINE HOAN THANH THANH CONG!")
    print("=" * 60 + "\n")
