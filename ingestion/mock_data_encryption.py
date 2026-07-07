import os
import time
import base64
import random
from datetime import timedelta
from pathlib import Path
import polars as pl
from faker import Faker
from dotenv import load_dotenv
from Crypto.Cipher import AES

load_dotenv()

ROOT_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT_DIR / "Data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

# MODULE 1: MÃ HÓA AES-256 (GCM MODE & BIẾN MÔI TRƯỜNG)
class AES256Cipher:
    def __init__(self): 
        readable_key = os.getenv("AES_SECRET_KEY")
        
        if not readable_key:
            raise ValueError("LỖI BẢO MẬT: Không tìm thấy khóa AES_SECRET_KEY trong file .env!")
            
        self.key = base64.b64decode(readable_key)

    def encrypt(self, data) -> str:
        """Mã hóa giá trị int thành chuỗi Base64 bằng chuẩn AEAD (AES-256-GCM)"""
        if data is None:
            return ""
        
        plain_bytes = str(data).encode('utf-8')
        
        cipher = AES.new(self.key, AES.MODE_GCM)
        ciphertext, tag = cipher.encrypt_and_digest(plain_bytes)
        
        combined_data = cipher.nonce + tag + ciphertext
        return base64.b64encode(combined_data).decode('utf-8')
    
    def decrypt(self, encrypted_str: str) -> str:
        """Giải mã chuỗi Base64 trả về giá trị gốc, đảm bảo tính toàn vẹn (AEAD)"""
        if not encrypted_str: return ""
        raw_data = base64.b64decode(encrypted_str)
        
        nonce, tag, ciphertext = raw_data[:16], raw_data[16:32], raw_data[32:]
        
        cipher = AES.new(self.key, AES.MODE_GCM, nonce=nonce)
        decrypted_bytes = cipher.decrypt_and_verify(ciphertext, tag)
        return decrypted_bytes.decode('utf-8')

# MODULE 2: PIPELINE SINH DỮ LIỆU & MÃ HÓA
def generate_and_encrypt_pipeline(num_rows: int) -> pl.DataFrame:
    #print(f"[*] Đang khởi tạo bộ dữ liệu giả lập với {num_rows:,} dòng (Quá trình này tốn vài phút do thư viện Faker)...")
    fake = Faker('vi_VN')
    #Faker.seed(42)
    #random.seed(42)

    PRODUCTS = {
        "Điện tử": [("EL01", "Smartphone"), ("EL02", "Laptop"), ("EL03", "Tablet"), ("EL04", "Smartwatch"), ("EL05", "Tai nghe")],
        "Thời trang": [("FA01", "Áo thun"), ("FA02", "Quần Jeans"), ("FA03", "Giày Sneaker"), ("FA04", "Áo khoác"), ("FA05", "Váy")],
        "Gia dụng": [("HO01", "Lò vi sóng"), ("HO02", "Tủ lạnh"), ("HO03", "Máy xay sinh tố"), ("HO04", "Điều hòa"), ("HO05", "Máy hút bụi")],
        "Mỹ phẩm": [("CO01", "Son môi"), ("CO02", "Kem nền"), ("CO03", "Serum"), ("CO04", "Sữa rửa mặt"), ("CO05", "Kem chống nắng")],
        "Thực phẩm": [("GR01", "Gạo ST25"), ("GR02", "Sữa tươi"), ("GR03", "Bánh mì"), ("GR04", "Táo nhập khẩu"), ("GR05", "Cà phê")]
    }
    
    raw_data = []
    current_rows = 0
    categories = list(PRODUCTS.keys())
    
    # Logic tạo đơn hàng chứa nhiều sản phẩm
    while current_rows < num_rows:
        # 1. KHỞI TẠO DỮ LIỆU CẤP ĐỘ ĐƠN HÀNG (Order-level) - GIỮ NGUYÊN CHO CÁC SẢN PHẨM CÙNG ĐƠN
        order_id = fake.unique.random_int(min=10000000, max=99999999)
        order_status = fake.random_element(elements=("Thành công", "Đã hủy", "Bị hoàn trả"))
        order_date_time_obj = fake.date_time_this_year()
        order_date_time = order_date_time_obj.strftime("%Y-%m-%d %H:%M:%S")
        
        customer_id = fake.random_int(min=10000, max=99999)
        customer_phone = fake.phone_number()
        customer_gender = fake.random_element(elements=("Nam", "Nữ", "Khác"))
        customer_age = fake.random_int(min=15, max=75)
        postal_code = fake.random_int(min=10000, max=99999) 
        device_agent = fake.user_agent()
        
        is_discount_applied = fake.boolean()
        reward_points = fake.random_int(min=0, max=1000)
        shipping_fee = fake.random_int(min=15000, max=150000)
        payment_method = fake.random_element(elements=("COD", "Ví điện tử", "Thẻ tín dụng", "Chuyển khoản"))
        shipping_carrier = fake.random_element(elements=("Giao Hàng Tiết Kiệm", "Shopee Express", "Viettel Post", "J&T Express"))
        
        # Thời gian giao hàng = Thời gian đặt + ngẫu nhiên 1 đến 5 ngày
        delivery_date_time = (order_date_time_obj + timedelta(days=random.randint(1, 5))).strftime("%Y-%m-%d %H:%M:%S")

        # Quyết định số lượng sản phẩm trong đơn hàng này (Tách từ 1 đến 4 dòng cho 1 hóa đơn)
        items_in_order = random.randint(1, 4)
        if current_rows + items_in_order > num_rows:
            items_in_order = num_rows - current_rows

        # 2. KHỞI TẠO DỮ LIỆU CẤP ĐỘ SẢN PHẨM (Line-level) - TÁCH DÒNG
        for _ in range(items_in_order):
            cat = random.choice(categories)
            prod = random.choice(PRODUCTS[cat])
            
            quantity = fake.random_int(min=1, max=5)
            mock_price = fake.random_int(min=50, max=5000) * 1000
            total_line_amount = mock_price * quantity
            
            raw_data.append({
                # Nhóm 1: Order Metadata
                "order_id": order_id,
                "order_status": order_status,
                "order_date_time": order_date_time,
                
                # Nhóm 2: Customer Info
                "customer_id": customer_id,
                "customer_phone": customer_phone,
                "customer_gender": customer_gender,
                "customer_age": customer_age,
                "postal_code": postal_code,
                "device_agent": device_agent,
                
                # Nhóm 3: Product Info
                "product_id": prod[0],
                "product_name": prod[1],
                "product_category": cat,
                
                # Nhóm 4: Financials & Metrics
                "quantity": quantity,
                "is_discount_applied": is_discount_applied,
                "reward_points": reward_points,
                "shipping_fee": shipping_fee,
                "total_line_amount": total_line_amount,
                
                # Nhóm 5: Payment & Logistics
                "payment_method": payment_method,
                "shipping_carrier": shipping_carrier,
                "delivery_date_time": delivery_date_time
            })
            current_rows += 1
    
    #print("[*] Đang thực thi Pipeline: Mã hóa ĐỒNG THỜI bằng map_batches...")

    cipher_tool = AES256Cipher()

    cols_to_encrypt = ["reward_points", "customer_age", "postal_code"]

    start_time = time.perf_counter()
    
    df_pipeline = (
        pl.DataFrame(raw_data)
        .with_columns([
            pl.col(col_name).map_batches(
                lambda s, _col=col_name: pl.Series([cipher_tool.encrypt(x) for x in s]), return_dtype=pl.String
            )
            for col_name in cols_to_encrypt
        ])
    )
    
    end_time = time.perf_counter()
    execution_time = end_time - start_time

    #print(f"\n Thời gian mã hóa thực tế (AES-256-GCM): {execution_time:.4f} giây")
    #print(f" Hiệu suất trung bình: {num_rows / execution_time:,.0f} dòng/giây\n")

    return df_pipeline

# CHƯƠNG TRÌNH CHÍNH (MAIN EXECUTION)
if __name__ == "__main__":
    NUM_ROWS = 500000
     
    pl.Config.set_tbl_width_chars(300)       
    pl.Config.set_fmt_str_lengths(15)        
    
    df_encrypted = generate_and_encrypt_pipeline(num_rows=NUM_ROWS)
    
    print("\n--- BẢNG DỮ LIỆU ĐÃ MÃ HÓA HOÀN CHỈNH ---")
    print(df_encrypted)
    
    print("\n--- CẤU TRÚC DỮ LIỆU ĐÃ MÃ HÓA (DẠNG DỌC) ---")
    print(df_encrypted.glimpse())

    parquet_filename = DATA_DIR / "Data_mock.parquet"
    df_encrypted.write_parquet(parquet_filename)
    
    print(f"\n Đã xuất {NUM_ROWS:,} dòng thành công ra file: {parquet_filename}")
    print(f" Dữ liệu đã được khóa bằng chuẩn AES-256-GCM. Quản lý key tập trung qua .env")

    df_encrypted.head(50).write_csv(DATA_DIR / "Data_mock_sample_50_rows.csv")

    print("\n--- TEST TÍNH NĂNG GIẢI MÃ ---")
    
    test_cipher = AES256Cipher()

    # Lấy thử chuỗi mã hóa của dòng đầu tiên trong cột reward_points (Điểm thưởng)
    sample_encrypted_reward = df_encrypted["reward_points"][0]
    
    print(f" Chuỗi đang lưu trong DB (Mã hóa): {sample_encrypted_reward}")
    
    decrypted_reward = test_cipher.decrypt(sample_encrypted_reward)
    print(f" Giá trị điểm thưởng thực tế sau khi giải mã: {decrypted_reward} điểm")
    print(" Quá trình mã hóa/giải mã 2 chiều hoạt động hoàn hảo!")