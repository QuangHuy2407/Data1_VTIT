import base64
import os
import polars as pl
from faker import Faker
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad, unpad

# MODULE 1: MÃ HÓA AES-256 (Sử dụng pycryptodome)
class AES256Cipher:
    def __init__(self, key: bytes = None):
        if key is None:
            self.key = os.urandom(32)
        else:
            self.key = key

    def encrypt(self, data: int) -> str:
        """Mã hóa một giá trị kiểu int thành chuỗi mã hóa Base64"""
        if data is None:
            return ""
        
        plain_bytes = str(data).encode('utf-8')
        iv = os.urandom(16)
        cipher = AES.new(self.key, AES.MODE_CBC, iv)
        padded_data = pad(plain_bytes, AES.block_size)
        encrypted_bytes = cipher.encrypt(padded_data)
        
        combined_data = iv + encrypted_bytes
        return base64.b64encode(combined_data).decode('utf-8')

    def decrypt(self, encrypted_str: str) -> int:
        """Hàm giải mã (Dùng để kiểm tra tính chính xác của dữ liệu sau khi mã hóa)"""
        if not encrypted_str:
            return None
        
        combined_bytes = base64.b64decode(encrypted_str.encode('utf-8'))
        iv = combined_bytes[:16]
        ciphertext = combined_bytes[16:]
        
        cipher = AES.new(self.key, AES.MODE_CBC, iv)
        decrypted_bytes = unpad(cipher.decrypt(ciphertext), AES.block_size)
        
        return int(decrypted_bytes.decode('utf-8'))

# MODULE 2: MOCK DỮ LIỆU GIẢ LẬP (Sử dụng Faker & Polars)
def generate_mock_data(num_rows: int = 100) -> pl.DataFrame:
    fake = Faker('vi_VN')
    Faker.seed(42)
    
    raw_data = []
    for _ in range(num_rows):
        row = {
            "customer_id": fake.random_int(min=100000, max=999999),  
            "full_name": fake.name(),
            "age": fake.random_int(min=18, max=75),                  
            "email": fake.email(),
            "phone_number": fake.phone_number(),
            "address": fake.address().replace("\n", ", "),
            "city": fake.city(),
            "country": "Vietnam",
            "job_title": fake.job(),
            "company": fake.company(),
            "monthly_spend": fake.random_int(min=1000, max=50000),   
            "signup_date": fake.date(),
            "last_login": fake.date_time().strftime("%Y-%m-%d %H:%M:%S"),
            "membership_level": fake.random_element(elements=("Bronze", "Silver", "Gold", "Diamond")),
            "is_active": fake.boolean(chance_of_getting_true=85),
            "ip_address": fake.ipv4(),
            "mac_address": fake.mac_address(),
            "user_agent": fake.user_agent(),
            "preferred_language": "Tiếng Việt",
            "total_orders": fake.random_int(min=1, max=150)           
        }
        raw_data.append(row)
        
    return pl.DataFrame(raw_data)

# CHƯƠNG TRÌNH CHÍNH (MAIN EXECUTION)
if __name__ == "__main__":
    print("=== BƯỚC 1: ĐANG SINH DỮ LIỆU GIẢ LẬP VỚI POLARS ===")
    # Đã sửa thành 50 dòng ở đây
    df_original = generate_mock_data(num_rows=50) 
    
    print("\n--- BẢNG DỮ LIỆU GỐC BAN ĐẦU ---")
    print(df_original)
    
    print("\n--- CẤU TRÚC 20 TRƯỜNG CỦA DỮ LIỆU GỐC (Dạng dọc) ---")
    print(df_original.glimpse())
    
    print("\n=== BƯỚC 2: KHỞI TẠO MODULE AES-256 ===")
    STATIC_KEY = b"12345678901234567890123456789012" 
    cipher_tool = AES256Cipher(key=STATIC_KEY)
    
    print("\n=== BƯỚC 3: TIẾN HÀNH MÃ HÓA TRỰC TIẾP TRÊN POLARS DATAFRAME ===")
    df_encrypted = df_original.with_columns([
        pl.col("customer_id").map_elements(cipher_tool.encrypt, return_dtype=pl.String).alias("customer_id"),
        pl.col("age").map_elements(cipher_tool.encrypt, return_dtype=pl.String).alias("age"),
        pl.col("monthly_spend").map_elements(cipher_tool.encrypt, return_dtype=pl.String).alias("monthly_spend"),
        pl.col("total_orders").map_elements(cipher_tool.encrypt, return_dtype=pl.String).alias("total_orders")
    ])
    
    print("\n--- BẢNG DỮ LIỆU SAU KHI MÃ HÓA ---")
    print(df_encrypted)
    
    print("\n--- CẤU TRÚC SAU KHI MÃ HÓA (Dạng dọc) ---")
    print(">>> Lưu ý: Cả 4 trường customer_id, age, monthly_spend, total_orders đã chuyển thành chuỗi (String)")
    print(df_encrypted.glimpse())
    
    print("\n=== BƯỚC 4: TEST THỬ LOGIC GIẢI MÃ ĐỂ KIỂM TRA ĐỘ CHÍNH XÁC ===")
    sample_encrypted_age = df_encrypted["age"][0]
    decrypted_age = cipher_tool.decrypt(sample_encrypted_age)
    print(f"Giá trị tuổi đã mã hóa ở dòng đầu tiên : {sample_encrypted_age}")
    print(f"Giá trị tuổi sau khi giải mã ngược lại : {decrypted_age} (Dữ liệu gốc ban đầu: {df_original['age'][0]})")

    print("\n=== BƯỚC 5: TEST TOÀN DIỆN - GIẢI MÃ NGƯỢC TOÀN BỘ BẢNG ===")
    df_decrypted = df_encrypted.with_columns([
        pl.col("customer_id").map_elements(cipher_tool.decrypt, return_dtype=pl.Int64).alias("customer_id"),
        pl.col("age").map_elements(cipher_tool.decrypt, return_dtype=pl.Int64).alias("age"),
        pl.col("monthly_spend").map_elements(cipher_tool.decrypt, return_dtype=pl.Int64).alias("monthly_spend"),
        pl.col("total_orders").map_elements(cipher_tool.decrypt, return_dtype=pl.Int64).alias("total_orders")
    ])

    print("\n--- BẢNG DỮ LIỆU SAU KHI ĐƯỢC KHÔI PHỤC (GIẢI MÃ) ---")
    print(df_decrypted)
    
    print("\n--- CẤU TRÚC SAU KHI KHÔI PHỤC (Dạng dọc) ---")
    print(df_decrypted.glimpse())

    print("\n=== BƯỚC 6: XUẤT FILE PARQUET ĐỂ LƯU TRỮ VÀ BÀN GIAO ===")
    parquet_filename = "encrypted_customer_data.parquet"
    df_encrypted.write_parquet(parquet_filename)
    
    print(f"✅ Đã xuất dữ liệu thành công ra file: {parquet_filename}")
    current_dir = os.path.dirname(os.path.abspath(__file__))
    print(f"📂 Vị trí lưu file: {os.path.join(current_dir, parquet_filename)}")