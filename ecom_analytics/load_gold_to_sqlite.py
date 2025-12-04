"""
Script load dữ liệu từ MinIO Gold layer vào SQLite database
Để dễ dàng query và visualize với Plotly
"""
import os
import io
import pandas as pd
import sqlite3
import boto3
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(__file__)
load_dotenv(os.path.join(BASE_DIR, "config", ".env"))

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = os.getenv("MINIO_BUCKET")
MINIO_REGION = os.getenv("MINIO_REGION", "us-east-1")

# SQLite database path
DB_PATH = os.path.join(BASE_DIR, "..", "ecom_analytics.db")

def get_s3_client():
    """Tạo S3 client để kết nối MinIO"""
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name=MINIO_REGION,
    )

def read_csv_from_minio(s3_client, key: str) -> pd.DataFrame:
    """Đọc CSV từ MinIO"""
    response = s3_client.get_object(Bucket=MINIO_BUCKET, Key=key)
    return pd.read_csv(io.BytesIO(response['Body'].read()), low_memory=False)

def main():
    print("=" * 80)
    print("📦 LOAD GOLD DATA TỪ MINIO VÀO SQLITE")
    print("=" * 80)
    
    # Kết nối MinIO
    s3_client = get_s3_client()
    
    # Kết nối SQLite (tạo mới nếu chưa có)
    conn = sqlite3.connect(DB_PATH)
    print(f"\n✅ Connected to SQLite: {DB_PATH}")
    
    # Định nghĩa các bảng Gold cần load
    gold_tables = {
        "agg_daily_sales": "gold/agg_daily_sales/data.csv",
        "agg_product_performance": "gold/agg_product_performance/data.csv",
        "agg_customer_lifetime": "gold/agg_customer_lifetime/data.csv",
        "agg_channel_performance": "gold/agg_channel_performance/data.csv",
        "customer_segment": "gold/customer_segment/data.csv",
    }
    
    # Load từng bảng
    for table_name, s3_key in gold_tables.items():
        print(f"\n📖 Loading {table_name}...")
        try:
            # Đọc từ MinIO
            df = read_csv_from_minio(s3_client, s3_key)
            
            # Parse dates nếu có
            date_cols = [col for col in df.columns if 'date' in col.lower()]
            for col in date_cols:
                df[col] = pd.to_datetime(df[col], errors='coerce')
            
            # Write vào SQLite
            df.to_sql(table_name, conn, if_exists='replace', index=False)
            print(f"   ✅ Loaded {len(df)} rows into table '{table_name}'")
            
        except Exception as e:
            print(f"   ⚠️ Error loading {table_name}: {e}")
    
    # Verify tables
    print("\n" + "=" * 80)
    print("📊 TABLES IN DATABASE:")
    print("=" * 80)
    
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    
    for table in tables:
        table_name = table[0]
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        print(f"   • {table_name}: {count} rows")
    
    conn.close()
    
    print("\n" + "=" * 80)
    print("✅ HOÀN THÀNH! Database đã sẵn sàng cho visualization")
    print("=" * 80)
    print(f"\n📁 Database location: {DB_PATH}")
    print("\n💡 Tiếp theo: Chạy script tạo dashboard")
    print("   python create_ecom_dashboards.py")

if __name__ == "__main__":
    main()
