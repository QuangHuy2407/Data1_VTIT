# 🚀 Hướng dẫn Triển khai Docker & Cài đặt Dịch vụ

## 📋 Yêu cầu trước khi bắt đầu

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) đã cài và **đang chạy**
- Tạo thư mục lưu trữ cho MinIO:

```powershell
mkdir D:\minio_data
```

---

## ⚡ Bước 1 – Khởi động toàn bộ hệ thống

```bash
docker-compose up -d
```

Kiểm tra các container đã lên chưa:

```bash
docker ps
```

Kết quả mong đợi — 3 container đều **Up**:

```
NAMES             STATUS
superset-app      Up ...
clickhouse-app    Up ... (healthy)
minio-app         Up ... (healthy)
```

> ⏳ Chờ khoảng **30–60 giây** để ClickHouse khởi động xong trước khi làm bước tiếp theo.

---

## 🧹 Bước 2 – Làm sạch DB cũ (Nếu đã từng chạy trước đó)

Nếu container Superset đã được khởi động trước đó và gặp lỗi, hãy xóa file DB cũ để bắt đầu mới tinh:

**Lệnh 1 – Xóa file DB cũ bên trong container:**

```bash
docker exec -it superset-app rm -f /app/superset_home/superset.db
```

**Lệnh 2 – Chạy lại cấu trúc Database mới tinh:**

```bash
docker exec -it superset-app superset db upgrade
```

> 💡 Nếu là lần đầu chạy và chưa có DB cũ, có thể bỏ qua Lệnh 1 và chạy thẳng Lệnh 2.

---

## 🔧 Bước 3 – Thiết lập Apache Superset (chỉ làm 1 lần)

Superset cần được khởi tạo thủ công sau khi container lên. Chạy **tuần tự** các lệnh sau:

### 3.1 – Khởi tạo database nội bộ (nếu chưa chạy ở Bước 2)

```bash
docker exec -it superset-app superset db upgrade
```

### 3.2 – Tạo tài khoản admin

```bash
docker exec -it superset-app superset fab create-admin --username admin --firstname Admin --lastname User --email admin@superset.com --password admin
```

> 💡 Nếu thành công sẽ hiện: `Admin User admin created.`

### 3.3 – Kích hoạt cấu hình (bắt buộc)

```bash
docker exec -it superset-app superset init
```

### 3.4 – Cài driver kết nối ClickHouse

```bash
docker exec -it superset-app pip install clickhouse-connect
```

---

## 🔗 Bước 4 – Kết nối Superset với ClickHouse

1. Mở trình duyệt vào **http://localhost:8088**
2. Đăng nhập: `admin` / `admin`
3. Vào **Settings → Database Connections → + Database**
4. Chọn database type: **ClickHouse Connect**
5. Nhập chuỗi kết nối:

```
clickhousedb+connect://admin:clickhouse123@clickhouse:8123/pipeline_db
```

> ⚠️ Dùng hostname `clickhouse` (tên service trong Docker network), **không dùng** `localhost`.

6. Nhấn **Test Connection** → **Connect**

---

## 🗄️ Bước 5 – Nạp dữ liệu từ MinIO vào ClickHouse

Sau khi file `data.parquet` đã được đẩy lên MinIO bucket `pipeline-bucket`, chạy câu SQL sau trong ClickHouse (qua HTTP hoặc client):

```sql
-- Tạo bảng và nạp dữ liệu trực tiếp từ MinIO
CREATE TABLE IF NOT EXISTS pipeline_db.encrypted_data
ENGINE = MergeTree()
ORDER BY id
AS
SELECT *
FROM s3(
    'http://minio:9000/pipeline-bucket/data.parquet',
    'admin',
    'password123',
    'Parquet'
);
```

> 💡 Dùng hostname `minio:9000` (tên service trong Docker network).

---

## 🌐 Thông tin truy cập các dịch vụ

| Dịch vụ          | URL                          | Tài khoản             | Mật khẩu       |
|------------------|------------------------------|-----------------------|----------------|
| MinIO Console    | http://localhost:9001        | `admin`               | `password123`  |
| ClickHouse HTTP  | http://localhost:8123        | `admin`               | `clickhouse123`|
| Apache Superset  | http://localhost:8088        | `admin`               | `admin`        |

---

## 🛠️ Các lệnh hay dùng

```bash
# Xem log của một service
docker logs clickhouse-app -f
docker logs superset-app -f
docker logs minio-app -f

# Restart một service
docker-compose restart clickhouse

# Dừng toàn bộ (giữ nguyên data)
docker-compose down

# Dừng và XÓA toàn bộ data (reset sạch)
docker-compose down -v
```
