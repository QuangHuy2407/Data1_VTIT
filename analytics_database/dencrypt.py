import os
import base64
from datetime import timezone
from zoneinfo import ZoneInfo
from pathlib import Path
from dotenv import load_dotenv
import clickhouse_connect
from Crypto.Cipher import AES
from concurrent.futures import ThreadPoolExecutor

# Cấu hình múi giờ hệ thống đồng bộ với Pipeline
VN_TZ = ZoneInfo("Asia/Ho_Chi_Minh")

# 1. Load các biến môi trường từ file .env (đường dẫn tuyệt đối cùng thư mục với script)
_ENV_PATH = Path(__file__).parent / ".env"
load_dotenv(dotenv_path=_ENV_PATH)

# 2. Lấy thông tin kết nối và Key mã hóa từ .env
CLICKHOUSE_HOST = "localhost"
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", 8123))
CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD")

# Loại bỏ phần text rác/đường dẫn thực thi thừa ở phía sau Key trong file .env (nếu có)
ENV_AES_KEY = os.getenv("AES_SECRET_KEY", "").split(";")[0].strip()
if not ENV_AES_KEY:
    raise ValueError("LỖI BẢO MẬT: Không tìm thấy khóa AES_SECRET_KEY trong file .env!")

AES_KEY = base64.b64decode(ENV_AES_KEY)


# 3. Hàm giải mã chuẩn AES-256 GCM 
def decrypt_aes256_gcm(cipher_text) -> str:
    try:
        if cipher_text is None:
            return ""

        if isinstance(cipher_text, str):
            cipher_text = cipher_text.strip()
            if cipher_text in ['', 'None', 'NULL', 'ᴺᵁᴸᴸ']:
                return ""

            # Khắc phục hiện tượng thay đổi ký tự '+' thành khoảng trắng
            cipher_text = cipher_text.replace(' ', '+')

            # Tự động vá lỗi thiếu ký tự padding '='
            missing_padding = len(cipher_text) % 4
            if missing_padding:
                cipher_text += '=' * (4 - missing_padding)

            raw_data = base64.b64decode(cipher_text)
        elif isinstance(cipher_text, bytes):
            raw_data = cipher_text
        else:
            return ""

        if len(raw_data) < 33:
            return ""

        nonce = raw_data[:16]
        tag = raw_data[16:32]
        ciphertext = raw_data[32:]

        cipher = AES.new(AES_KEY, AES.MODE_GCM, nonce=nonce)
        decrypted_bytes = cipher.decrypt_and_verify(ciphertext, tag)

        return decrypted_bytes.decode('utf-8').strip()
    except Exception:
        return ""


# Hàm hỗ trợ xử lý đa luồng cho từng dòng
def process_single_row(row):
    order_id, order_status, order_date_time = str(row[0]), str(row[1]), row[2]
    customer_id, customer_phone, customer_gender = str(row[3]), str(row[4]), str(row[5])
    customer_age_base64, postal_code_base64 = row[6], row[7]
    device_agent, product_id, product_name, product_category = str(row[8]), str(row[9]), str(row[10]), str(row[11])
    quantity, is_discount_applied = int(row[12]), (1 if row[13] else 0)
    reward_points_base64 = row[14]
    shipping_fee, total_line_amount = float(row[15]), float(row[16])

    payment_method, shipping_carrier, delivery_date_time, stored_at = str(row[17]), str(row[18]), row[19], row[20]

    decrypted_age_str = decrypt_aes256_gcm(customer_age_base64)
    decrypted_postal_str = decrypt_aes256_gcm(postal_code_base64)
    decrypted_reward_str = decrypt_aes256_gcm(reward_points_base64)

    is_success = bool(decrypted_age_str and decrypted_postal_str)

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

    processed_record = [
        order_id, order_status, order_date_time, customer_id, customer_phone,
        customer_gender, customer_age, postal_code, device_agent, product_id,
        product_name, product_category, quantity, is_discount_applied, reward_points,
        shipping_fee, total_line_amount, payment_method, shipping_carrier, delivery_date_time, stored_at
    ]
    
    return is_success, processed_record


# 4. Hàm main thực thi đồng bộ với Pipeline
def main(since_datetime=None):
    client = clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        user=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DB
    )

    # --- ĐỒNG BỘ CẤU TRÚC BẢNG THEO FILE CỦA BẠN (MergeTree) ---
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

    select_fields = """
        order_id, order_status, order_date_time, customer_id, customer_phone, 
        customer_gender, customer_age, postal_code, device_agent, product_id, 
        product_name, product_category, quantity, is_discount_applied, reward_points, 
        shipping_fee, total_line_amount, payment_method, shipping_carrier, delivery_date_time, stored_at
    """
    base_where = "WHERE customer_age IS NOT NULL AND customer_age != '' AND postal_code IS NOT NULL AND postal_code != ''"

    # Xử lý logic thời gian Delta Load
    if since_datetime is not None:
        since_vn = since_datetime.astimezone(VN_TZ)
        formatted_since = since_vn.strftime("%Y-%m-%d %H:%M:%S")
        print(f"[*] Chế độ PIPELINE: Chỉ giải mã dữ liệu mới insert sau mốc (giờ VN): {formatted_since}...")
        query = f"SELECT {select_fields} FROM Customer.customer_data {base_where} AND stored_at > '{formatted_since}'"
    else:
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

    # --- GIẢI MÃ ĐA LUỒNG ---
    print("[*] Đang tiến hành giải mã đa luồng song song...")
    with ThreadPoolExecutor() as executor:
        processing_results = executor.map(process_single_row, result.result_rows)
        
        for is_success, row_data in processing_results:
            if is_success:
                success_count += 1
            else:
                fail_count += 1
            data_to_insert.append(row_data)

    print(f"--- Giải mã thành công: {success_count:,} / {total_rows:,} dòng | Thất bại: {fail_count:,} dòng ---")

    # --- GHI BẢN GHI VÀO CLICKHOUSE THEO CHUNK ---
    if data_to_insert:
        column_names = [
            'order_id', 'order_status', 'order_date_time', 'customer_id', 'customer_phone',
            'customer_gender', 'customer_age', 'postal_code', 'device_agent', 'product_id',
            'product_name', 'product_category', 'quantity', 'is_discount_applied', 'reward_points',
            'shipping_fee', 'total_line_amount', 'payment_method', 'shipping_carrier', 'delivery_date_time', 'stored_at'
        ]
        
        batch_size = 100000

        total_records = len(data_to_insert)
        
        print(f"--- Đang thực hiện chia nhỏ và bulk insert {total_records:,} dòng vào ClickHouse... ---")
        for i in range(0, total_records, batch_size):
            current_batch = data_to_insert[i:i + batch_size]
            client.insert('Customer.customer_data_giai', current_batch, column_names=column_names)
            
        print("--- Hoàn tất tiến trình giải mã hệ thống! ---")


if __name__ == "__main__":
    main()