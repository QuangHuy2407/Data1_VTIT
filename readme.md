# KẾ HOẠCH TRIỂN KHAI DỰ ÁN: DATA PIPELINE CÓ MÃ HÓA DỮ LIỆU

## 🛠️ 1. SƠ ĐỒ KIẾN TRÚC HỆ THỐNG (DATA FLOW)

Luồng dữ liệu di chuyển tuần tự qua các bước sau:
[Polars (Mock 20 trường)] ──> [Mã hóa AES-256 (3 trường int)] ──> [MinIO (Định dạng Parquet)] ──> [ClickHouse (OLAP Database)] ──> [SQL Decrypt / Apache Superset (Dashboard)]

---

## 👥 2. SƠ ĐỒ PHÂN CHIA CÔNG VIỆC CHI TIẾT

Để đảm bảo tính công bằng tuyệt đối về khối lượng công việc và kỹ thuật, dự án được chia theo trục dọc dữ liệu. Mỗi thành viên tự phụ trách một module độc lập từ Code, Tích hợp hệ thống cho đến Viết báo cáo.

### 🧑‍💻 THÀNH VIÊN A: PHỤ TRÁCH KHỞI TẠO & MÃ HÓA (PIPELINE ĐẦU VÀO)
*   **Công việc Kỹ thuật:**
    *   Viết script Python kết hợp thư viện Polars và Faker để sinh dữ liệu giả lập (tối thiểu 20 trường).
    *   Nghiên cứu và viết module mã hóa AES-256 (sử dụng thư viện `pycryptodome`).
    *   Thực hiện mã hóa trực tiếp trên Polars DataFrame đối với ít nhất 3 trường kiểu `int` đã chọn.
*   **Nội dung viết Báo cáo:**
    *   **Mục 1:** Thiết kế Schema dữ liệu (Mô tả chi tiết danh sách 20 trường, kiểu dữ liệu ban đầu).
    *   **Mục 2:** Giải pháp bảo mật dữ liệu với AES-256 (Giải thích thuật toán, chế độ mã hóa, cách quản lý Secret Key/IV và chụp ảnh minh chứng đoạn code chạy thành công).

### 🪛 THÀNH VIÊN B: PHỤ TRÁCH HẠ TẦNG & DỊCH CHUYỂN (DATA LAKE & WAREHOUSE)
*   **Công việc Kỹ thuật:**
    *   Viết file `docker-compose.yml` để dựng và cấu hình mạng cho toàn bộ hệ sinh thái (MinIO, ClickHouse, Apache Superset).
    *   Nhận dữ liệu từ Thành viên A, viết mã kết nối để đẩy file dữ liệu định dạng `.parquet` lên MinIO Bucket.
    *   Khởi tạo cấu trúc bảng trên ClickHouse và cấu hình luồng nạp dữ liệu từ MinIO về ClickHouse thông qua hàm `s3()`.
*   **Nội dung viết Báo cáo:**
    *   **Mục 3:** Kiến trúc hạ tầng hệ thống (Giải thích file cấu hình `docker-compose.yml`).
    *   **Mục 4:** Lưu trữ và tích hợp dữ liệu (Mô tả cách lưu trên MinIO, câu lệnh SQL nạp dữ liệu vào ClickHouse kèm ảnh chụp bảng dữ liệu mã hóa đã nạp thành công).

### 📊 THÀNH VIÊN C: PHỤ TRÁCH GIẢI MÃ & TRỰC QUAN (BI & ANALYTICS)
*   **Công việc Kỹ thuật:**
    *   Thiết lập kết nối (Database Connection) giữa Apache Superset và ClickHouse.
    *   Nghiên cứu và viết các câu lệnh SQL giải mã (Decrypt) trực tiếp trong ClickHouse hoặc trên Superset bằng cách sử dụng chung Secret Key với Thành viên A để lấy lại số nguyên gốc phục vụ tính toán (Sum, Avg).
    *   Xây dựng, thiết kế và tối ưu giao diện Dashboard hoàn chỉnh trên Apache Superset.
*   **Nội dung viết Báo cáo:**
    *   **Mục 5:** Giải pháp giải mã dữ liệu On-the-fly (Lúc truy vấn) để phục vụ cho mục đích phân tích.
    *   **Mục 6:** Trực quan hóa dữ liệu (Giải thích ý nghĩa các biểu đồ trên Dashboard kèm ảnh chụp giao diện Superset hoàn chỉnh).

---

## 📅 3. TIẾN ĐỘ TRIỂN KHAI TRONG 1 TUẦN (7 NGÀY)

### ⏱️ Giai đoạn 1: Xây dựng nền móng (Ngày 1 - Ngày 2)
*   **Thành viên A:** Hoàn thành script sinh 20 trường dữ liệu bằng Polars và viết xong hàm mã hóa AES-256. Chụp lại ảnh code làm tư liệu báo cáo.
*   **Thành viên B:** Dựng xong môi trường Docker (MinIO, ClickHouse, Superset), đảm bảo các dịch vụ thông suốt với nhau. Chụp lại ảnh file docker-compose.
*   **Thành viên C:** Tạo sẵn file Google Docs báo cáo chung của nhóm, chuẩn bị cấu trúc định dạng font, lề lối và gửi link cho cả nhóm.

### ⏱️ Giai đoạn 2: Thông luồng hệ thống (Ngày 3 - Ngày 4)
*   **Thành viên A & B:** Phối hợp đẩy dữ liệu từ Polars lên MinIO và kéo sang ClickHouse. Kiểm tra đảm bảo các trường `int` hiển thị ở dạng chuỗi mã hóa vô nghĩa trong DB.
*   **Thành viên C:** Nhận Secret Key từ Thành viên A, viết và thử nghiệm các câu lệnh SQL giải mã trên ClickHouse để chuẩn bị dữ liệu cho Superset.
*   **Báo cáo:** Thành viên A hoàn thiện Mục 1 & 2; Thành viên B hoàn thiện Mục 3 & 4 vào file báo cáo chung.

### ⏱️ Giai đoạn 3: Hoàn thiện Dashboard & Gom bài (Ngày 5 - Ngày 6)
*   **Thành viên C:** Thực hiện kết nối Superset với ClickHouse, áp dụng câu lệnh SQL giải mã dữ liệu và tiến hành kéo thả, thiết kế hoàn chỉnh Dashboard. Chụp ảnh màn hình giao diện.
*   **Báo cáo:** Thành viên C hoàn thiện Mục 5 & 6 vào file báo cáo chung.

### ⏱️ Giai đoạn 4: Tổng duyệt & Nghiệm thu (Ngày 7)
*   **Cả nhóm:** Chạy lại toàn bộ luồng dữ liệu (End-to-End) một lần cuối để đảm bảo không lỗi.
*   **Cả nhóm:** Ngồi lại cùng nhau rà soát lại toàn bộ file báo cáo, sửa lỗi chính tả, bổ sung phần *Mở đầu*, *Kết luận*, *Bảng phân công nhiệm vụ* và xuất file PDF để sẵn sàng nộp bài.