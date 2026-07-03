import os
import time
import base64
import polars as pl
from faker import Faker
from dotenv import load_dotenv
from Crypto.Cipher import AES

load_dotenv()

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
    print(f"[*] Đang khởi tạo bộ dữ liệu giả lập với {num_rows:,} dòng (Quá trình này tốn vài phút do thư viện Faker)...")
    fake = Faker('vi_VN')
    Faker.seed(42)
    
    cipher_tool = AES256Cipher()
    print("[*] Đã kết nối thành công với Secret Key từ hệ thống (.env).")

    raw_data = []
    for _ in range(num_rows):
        raw_data.append({
            "customer_id": fake.unique.random_int(min=1000000, max=9999999), 
            "total_orders": fake.random_int(min=1, max=500),                     
            "reward_points": fake.random_int(min=0, max=10000),                  
            "cccd_number": fake.random_int(min=100000000000, max=999999999999),
            "account_balance": fake.random_int(min=10_000_000, max=500_000_000), 
            "card_cvv": fake.random_int(min=100, max=999),     
            "full_name": fake.name(),
            "email": fake.free_email(),
            "phone_number": fake.phone_number(),
            "address": fake.address().replace("\n", ", "),
            "city": fake.city(),
            "job_title": fake.job(),
            "company": fake.company(),
            "signup_date": fake.date_between(start_date='-5y', end_date='today').strftime("%Y-%m-%d"),
            "last_login": fake.date_time_this_month().strftime("%Y-%m-%d %H:%M:%S"),
            "membership_level": fake.random_element(elements=("Standard", "Silver", "Gold", "Platinum")),
            "is_active": fake.boolean(chance_of_getting_true=80),
            "ip_address": fake.ipv4(),
            "mac_address": fake.mac_address(),
            "device_agent": fake.user_agent()
        })
    
    print("[*] Đang thực thi Pipeline: Mã hóa ĐỒNG THỜI bằng map_batches...")
    
    cols_to_encrypt = ["cccd_number", "account_balance", "card_cvv"]

    start_time = time.perf_counter()
    
    df_pipeline = (
        pl.DataFrame(raw_data)
        .with_columns([
            pl.col(col_name).map_batches(
                lambda s: pl.Series([cipher_tool.encrypt(x) for x in s]), return_dtype=pl.String
            )
            for col_name in cols_to_encrypt
        ])
    )
    
    end_time = time.perf_counter()
    execution_time = end_time - start_time

    print(f"\n🚀 Thời gian mã hóa thực tế (AES-256-GCM): {execution_time:.4f} giây")
    print(f"📊 Hiệu suất trung bình: {num_rows / execution_time:,.0f} dòng/giây\n")

    return df_pipeline

# CHƯƠNG TRÌNH CHÍNH (MAIN EXECUTION)
if __name__ == "__main__":
    NUM_ROWS = 1_000_000 
     
    pl.Config.set_tbl_width_chars(300)       
    pl.Config.set_fmt_str_lengths(15)        
    
    df_encrypted = generate_and_encrypt_pipeline(num_rows=NUM_ROWS)
    
    print("\n--- BẢNG DỮ LIỆU ĐÃ MÃ HÓA HOÀN CHỈNH ---")
    print(df_encrypted)
    
    print("\n--- CẤU TRÚC DỮ LIỆU ĐÃ MÃ HÓA (DẠNG DỌC) ---")
    print(df_encrypted.glimpse())

    parquet_filename = "encrypted_customer_data_1M.parquet"
    df_encrypted.write_parquet(parquet_filename)
    
    print(f"\n✅ Đã xuất {NUM_ROWS:,} dòng thành công ra file: {parquet_filename}")
    print(f"🔒 Dữ liệu đã được khóa bằng chuẩn AES-256-GCM. Quản lý key tập trung qua .env")

    df_encrypted.head(50).write_csv("sample_data_50_rows.csv")
    
    print("\n--- 🔍 TEST TÍNH NĂNG GIẢI MÃ (TASK 6) ---")
    
    test_cipher = AES256Cipher()

    sample_encrypted_balance = df_encrypted["account_balance"][0]
    
    print(f"🔒 Chuỗi đang lưu trong DB (Mã hóa): {sample_encrypted_balance}")
    
    decrypted_balance = test_cipher.decrypt(sample_encrypted_balance)
    print(f"🔓 Giá trị thực tế sau khi giải mã:  {decrypted_balance} VNĐ")
    print("✅ Quá trình mã hóa/giải mã 2 chiều hoạt động hoàn hảo!")