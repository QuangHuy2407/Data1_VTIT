#!/bin/bash

# Kiểm tra xem container Superset có đang chạy không
if [ ! "$(docker ps -q -f name=superset-app)" ]; then
    echo "❌ Lỗi: Container 'superset-app' chưa được khởi động."
    echo "Vui lòng chạy 'docker-compose up -d' trước!"
    exit 1
fi

echo "=========================================================="
echo "💥 ĐANG TIẾN HÀNH RESET & KHỞI TẠO LẠI SUPERSET..."
echo "=========================================================="

# 🧹 BƯỚC 2: Làm sạch DB cũ
echo "👉 1. Xóa file DB cũ bên trong container..."
docker exec -it superset-app rm -f /app/superset_home/superset.db

echo "👉 2. Chạy lại cấu trúc Database mới tinh..."
docker exec -it superset-app superset db upgrade

# 🔧 BƯỚC 3: Thiết lập Apache Superset
echo "👉 3. Tạo tài khoản admin mặc định..."
docker exec -it superset-app superset fab create-admin \
  --username admin \
  --firstname Admin \
  --lastname User \
  --email admin@superset.com \
  --password admin

echo "👉 4. Kích hoạt phân quyền hệ thống (superset init)..."
docker exec -it superset-app superset init

echo "👉 5. Cài đặt driver kết nối ClickHouse..."
docker exec -it superset-app pip install clickhouse-connect

echo "=========================================================="
echo "🎉 Xong! Đã reset và cấu hình xong Superset."
echo "Truy cập ngay: http://localhost:8088"
echo "=========================================================="