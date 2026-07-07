import os
import base64
from datetime import timezone
from zoneinfo import ZoneInfo

VN_TZ = ZoneInfo("Asia/Ho_Chi_Minh")
from pathlib import Path
from dotenv import load_dotenv
import clickhouse_connect
from Crypto.Cipher import AES

# 1. Load các biến môi trường từ file .env (đường dẫn tuyệt đối cùng thư mục với script)
_ENV_PATH = Path(__file__).parent / ".env"
load_dotenv(dotenv_path=_ENV_PATH)

# 2. Lấy thông tin kết nối và Key mã hóa từ .env
CLICKHOUSE_HOST = "localhost"
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT"))
CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD")

# Loại bỏ phần text rác/đường dẫn thực thi thừa ở phía sau Key trong file .env (nếu có)
ENV_AES_KEY = os.getenv("AES_SECRET_KEY", "").split(";")[0].strip()
if not ENV_AES_KEY:
    raise ValueError("LỖI BẢO MẬT: Không tìm thấy khóa AES_SECRET_KEY trong file .env!")

AES_KEY = base64.b64decode(ENV_AES_KEY)


# 3. Hàm giải mã chuẩn AES-256 GCM bằng PyCryptodome (Đồng bộ cấu trúc dữ liệu Polars)
def decrypt_aes256_gcm(cipher_text) -> str:
    try:
        if cipher_text is None:
            return ""

        if isinstance(cipher_text, str):
            cipher_text = cipher_text.strip()
            if cipher_text in ['', 'None', 'NULL', 'ᴺᵁᴸᴸ']:
                return ""

            # Khắc phục hiện tượng thay đổi ký tự '+' thành khoảng trắng khi lưu trữ/truyền tải qua HTTP
            cipher_text = cipher_text.replace(' ', '+')

            # Tự động vá lỗi thiếu ký tự padding '=' của định dạng Base64
            missing_padding = len(cipher_text) % 4
            if missing_padding:
                cipher_text += '=' * (4 - missing_padding)

            raw_data = base64.b64decode(cipher_text)
        elif isinstance(cipher_text, bytes):
            raw_data = cipher_text
        else:
            return ""

        # Kiểm tra độ dài tối thiểu (Nonce 16 bytes + Tag 16 bytes + tối thiểu 1 byte Ciphertext)
        if len(raw_data) < 33:
            return ""

        # Bóc tách chuẩn cấu trúc AEAD: Nonce (16B) -> Tag (16B) -> Ciphertext
        nonce = raw_data[:16]
        tag = raw_data[16:32]
        ciphertext = raw_data[32:]

        # Khởi tạo và xác thực tính toàn vẹn dữ liệu
        cipher = AES.new(AES_KEY, AES.MODE_GCM, nonce=nonce)
        decrypted_bytes = cipher.decrypt_and_verify(ciphertext, tag)

        return decrypted_bytes.decode('utf-8').strip()
    except Exception:
        # Ẩn log lỗi khi chạy thực tế để tăng tốc độ xử lý pipeline dữ liệu lớn
        return ""


# 4. Hàm main thực thi quét Delta dữ liệu và ghi kết quả vào ClickHouse
def main(since_datetime=None):
    """
    since_datetime: datetime | None
        - Nếu được truyền vào, chỉ giải mã các bản ghi có stored_at > since_datetime.
        - Nếu None, sử dụng cơ chế tự động: lấy MAX(stored_at) từ bảng đích (Delta Load cũ).
    """
    # Sửa đổi từ 'host_port' thành 'port' để tương thích chính xác với clickhouse_connect driver
    client = clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        user=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DB
    )

    # --- BƯỚC 1: KHỞI TẠO BẢNG ĐÍCH ---
    # Bạn có thể comment dòng DROP TABLE dưới đây nếu muốn lưu lũy tiến dữ liệu chạy hàng ngày
    #client.command("DROP TABLE IF EXISTS Customer.customer_data_giai")
    #print("--- Đã làm sạch cấu trúc bảng giải mã cũ ---")

    create_table_query = """
    CREATE TABLE IF NOT EXISTS Customer.customer_data_giai (
        order_id String,
        order_status String,
        order_date_time DateTime,
        customer_id String,
        customer_phone String,
        customer_gender String,
        customer_age Nullable(Int32),      
        postal_code Nullable(Int32),       
        device_agent String,
        product_id String,
        product_name String,
        product_category String,
        quantity Int32,
        is_discount_applied UInt8,
        reward_points Nullable(Int32),     
        shipping_fee Float64,
        total_line_amount Float64,
        payment_method String,
        shipping_carrier String,
        delivery_date_time DateTime,
        stored_at DateTime('Asia/Ho_Chi_Minh')
    ) ENGINE = MergeTree()
    ORDER BY (stored_at, order_id)          
    """
    client.command(create_table_query)

    # --- BƯỚC 2: XÁC ĐỊNH MỐC THỜI GIAN ĐỂ LỌC DỮ LIỆU MỚI (CƠ CHẾ DELTA LOAD) ---
    select_fields = """
        order_id, order_status, order_date_time, customer_id, customer_phone, 
        customer_gender, customer_age, postal_code, device_agent, product_id, 
        product_name, product_category, quantity, is_discount_applied, reward_points, 
        shipping_fee, total_line_amount, payment_method, shipping_carrier, delivery_date_time, stored_at
    """

    # Loại bỏ các bản ghi rỗng/NULL ở các trường mã hóa để tối ưu hóa bộ nhớ quét
    base_where = "WHERE customer_age IS NOT NULL AND customer_age != '' AND postal_code IS NOT NULL AND postal_code != ''"

    if since_datetime is not None:
        # Ưu tiên dùng mốc thời gian được pipeline truyền vào (chính xác nhất)
        # QUAN TRỌNG: Cột stored_at được khai báo là DateTime('Asia/Ho_Chi_Minh').
        # Khi dùng string literal trong SQL của ClickHouse, nó interpret theo timezone của cột.
        # Do đó phải format sang giờ VN (không phải UTC) để filter đúng.
        since_vn = since_datetime.astimezone(VN_TZ)
        formatted_since = since_vn.strftime("%Y-%m-%d %H:%M:%S")
        print(f"[*] Chế độ PIPELINE: Chỉ giải mã dữ liệu mới insert sau mốc (giờ VN): {formatted_since}...")
        query = f"SELECT {select_fields} FROM Customer.customer_data {base_where} AND stored_at > '{formatted_since}'"
    else:
        # Fallback: tự động lấy MAX(stored_at) từ bảng đích khi chạy standalone
        max_stored_at_query = "SELECT MAX(stored_at) FROM Customer.customer_data_giai"
        max_res = client.query(max_stored_at_query)
        max_stored_at = max_res.result_rows[0][0]

        is_first_run = True
        if max_stored_at and hasattr(max_stored_at, 'year'):
            if max_stored_at.year > 1970:
                is_first_run = False

        if is_first_run:
            print("[*] Bảng giải mã trống. Bắt đầu quét TOÀN BỘ dữ liệu từ bảng gốc...")
            query = f"SELECT {select_fields} FROM Customer.customer_data {base_where}"
        else:
            formatted_max_time = max_stored_at.strftime("%Y-%m-%d %H:%M:%S")
            print(f"[*] Đang quét phần dữ liệu mới tăng trưởng (Delta) từ mốc: {formatted_max_time}...")
            query = f"SELECT {select_fields} FROM Customer.customer_data {base_where} AND stored_at > '{formatted_max_time}'"

    result = client.query(query)
    total_rows = len(result.result_rows)
    print(f"--- Tìm thấy {total_rows:,} dòng dữ liệu cần giải mã ---")

    if total_rows == 0:
        print("--- Không tìm thấy dữ liệu mới. Tiến trình kết thúc! ---")
        return

    data_to_insert = []
    success_count = 0
    fail_count = 0

    # Duyệt qua từng dòng dữ liệu từ Clickhouse để giải mã nhị phân
    for row in result.result_rows:
        order_id, order_status, order_date_time = str(row[0]), str(row[1]), row[2]
        customer_id, customer_phone, customer_gender = str(row[3]), str(row[4]), str(row[5])
        customer_age_base64, postal_code_base64 = row[6], row[7]
        device_agent, product_id, product_name, product_category = str(row[8]), str(row[9]), str(row[10]), str(row[11])
        quantity, is_discount_applied = int(row[12]), (1 if row[13] else 0)
        reward_points_base64 = row[14]
        shipping_fee, total_line_amount = float(row[15]), float(row[16])
        payment_method, shipping_carrier, delivery_date_time, stored_at = str(row[17]), str(row[18]), row[19], row[20]

        # Thực thi giải mã ngược
        decrypted_age_str = decrypt_aes256_gcm(customer_age_base64)
        decrypted_postal_str = decrypt_aes256_gcm(postal_code_base64)
        decrypted_reward_str = decrypt_aes256_gcm(reward_points_base64)

        # Chỉ tính SUCCESS khi các trường BẮT BUỘC (age, postal_code) đều giải mã được
        if decrypted_age_str and decrypted_postal_str:
            success_count += 1
        else:
            fail_count += 1

        # Chuyển đổi an toàn về kiểu dữ liệu Int32 của ClickHouse
        try:
            customer_age = int(decrypted_age_str) if decrypted_age_str else None
        except ValueError:
            customer_age = None

        try:
            postal_code = int(decrypted_postal_str) if decrypted_postal_str else None
        except ValueError:
            postal_code = None

        try:
            reward_points = int(decrypted_reward_str) if decrypted_reward_str else None
        except ValueError:
            reward_points = None

        data_to_insert.append([
            order_id, order_status, order_date_time, customer_id, customer_phone,
            customer_gender, customer_age, postal_code, device_agent, product_id,
            product_name, product_category, quantity, is_discount_applied, reward_points,
            shipping_fee, total_line_amount, payment_method, shipping_carrier, delivery_date_time, stored_at
        ])

    print(f"--- Giải mã thành công: {success_count:,} / {total_rows:,} dòng | Thất bại: {fail_count:,} dòng ---")

    # --- BƯỚC 4: GHI BẢN GHI ĐÃ GIẢI MÃ VÀO CLICKHOUSE ---
    if data_to_insert:
        print(f"--- Đang thực hiện bulk insert {len(data_to_insert):,} dòng vào ClickHouse... ---")
        column_names = [
            'order_id', 'order_status', 'order_date_time', 'customer_id', 'customer_phone',
            'customer_gender', 'customer_age', 'postal_code', 'device_agent', 'product_id',
            'product_name', 'product_category', 'quantity', 'is_discount_applied', 'reward_points',
            'shipping_fee', 'total_line_amount', 'payment_method', 'shipping_carrier', 'delivery_date_time', 'stored_at'
        ]
        client.insert('Customer.customer_data_giai', data_to_insert, column_names=column_names)
        print("--- Hoàn tất tiến trình giải mã hệ thống! ---")


if __name__ == "__main__":
    main()