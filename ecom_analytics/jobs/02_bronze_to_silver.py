# jobs/02_bronze_to_silver.py
"""
Silver Layer: Clean và transform data từ Bronze (MinIO) lên Silver (MinIO).
- Đọc CSV từ MinIO bronze layer bằng Pandas
- Clean data: rename columns, parse dates, remove duplicates
- Upload CSV lên MinIO silver layer
"""
import os
import io
import pandas as pd
import numpy as np
import boto3
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.dirname(__file__))
load_dotenv(os.path.join(BASE_DIR, "config", ".env"))

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = os.getenv("MINIO_BUCKET")
MINIO_REGION = os.getenv("MINIO_REGION", "us-east-1")


def get_s3_client():
    """Tạo S3 client để kết nối MinIO"""
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name=MINIO_REGION,
    )


def get_latest_bronze_key(s3_client, prefix: str) -> str:
    """Lấy file CSV mới nhất từ bronze layer"""
    response = s3_client.list_objects_v2(Bucket=MINIO_BUCKET, Prefix=prefix)
    if 'Contents' not in response:
        raise FileNotFoundError(f"No files found in {prefix}")
    
    files = sorted(response['Contents'], key=lambda x: x['LastModified'], reverse=True)
    return files[0]['Key']


def read_csv_from_minio(s3_client, key: str) -> pd.DataFrame:
    """Đọc CSV từ MinIO"""
    print(f"  📥 Downloading: {key}")
    response = s3_client.get_object(Bucket=MINIO_BUCKET, Key=key)
    return pd.read_csv(io.BytesIO(response['Body'].read()), low_memory=False)


def upload_csv_to_minio(df: pd.DataFrame, key: str, s3_client):
    """Upload DataFrame dưới dạng CSV lên MinIO"""
    buffer = io.StringIO()
    df.to_csv(buffer, index=False)
    buffer.seek(0)
    
    s3_client.put_object(
        Bucket=MINIO_BUCKET,
        Key=key,
        Body=buffer.getvalue().encode("utf-8"),
        ContentType="text/csv",
    )
    print(f"  ✅ Uploaded to s3://{MINIO_BUCKET}/{key}")


def parse_datetime_column(series: pd.Series) -> pd.Series:
    """Parse datetime - xử lý cả ISO format và Unix timestamp (ms)"""
    # Tạo bản copy để tránh thay đổi original
    s = series.copy().astype(str)
    
    # Tạo mask cho các giá trị là số (Unix timestamp)
    is_numeric = s.str.match(r'^[\d.]+$', na=False)
    
    # Kết quả
    result = pd.Series([pd.NaT] * len(series), index=series.index)
    
    # Xử lý Unix timestamps
    if is_numeric.any():
        numeric_vals = pd.to_numeric(s[is_numeric], errors='coerce')
        # Convert milliseconds to datetime
        timestamps = pd.to_datetime(numeric_vals / 1000, unit='s', errors='coerce')
        result.loc[is_numeric] = timestamps
    
    # Xử lý ISO format strings
    if (~is_numeric).any():
        iso_vals = pd.to_datetime(s[~is_numeric], errors='coerce')
        result.loc[~is_numeric] = iso_vals
    
    return result


def clean_checkouts(s3_client):
    """Clean checkouts data bằng Pandas"""
    print("\n📖 Reading checkouts from bronze...")
    
    key = get_latest_bronze_key(s3_client, "bronze/checkouts/")
    df = read_csv_from_minio(s3_client, key)
    print(f"  → Raw rows: {len(df)}")
    
    # Combine createAt and createdAt into one column first
    if 'createAt' in df.columns and 'createdAt' in df.columns:
        df['created_at'] = df['createdAt'].combine_first(df['createAt'])
        df = df.drop(columns=['createAt', 'createdAt'])
    elif 'createAt' in df.columns:
        df = df.rename(columns={'createAt': 'created_at'})
    elif 'createdAt' in df.columns:
        df = df.rename(columns={'createdAt': 'created_at'})
    
    # Rename other columns
    rename_map = {
        "_id": "checkout_id",
        "customerId": "customer_id",
        "total": "total_amount",
        "currency": "currency",
        "channel": "channel",
        "email": "email",
        "status": "status",
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})
    
    # Parse datetime
    if 'created_at' in df.columns:
        df['created_at'] = parse_datetime_column(df['created_at'])
    
    # Lowercase email
    if 'email' in df.columns:
        df['email'] = df['email'].astype(str).str.lower().replace('nan', np.nan)
    
    # Remove duplicates
    if 'checkout_id' in df.columns:
        df = df.drop_duplicates(subset=['checkout_id'])
    
    # Select columns
    keep_cols = ['checkout_id', 'customer_id', 'email', 'created_at', 'total_amount', 
                 'currency', 'channel', 'status']
    keep_cols = [c for c in keep_cols if c in df.columns]
    df = df[keep_cols]
    
    # Upload
    upload_csv_to_minio(df, "silver/checkouts_clean/data.csv", s3_client)
    print(f"  → Clean rows: {len(df)}")
    return df


def clean_orders(s3_client):
    """Clean orders data bằng Pandas"""
    print("\n📖 Reading orders from bronze...")
    
    key = get_latest_bronze_key(s3_client, "bronze/orders/")
    df = read_csv_from_minio(s3_client, key)
    print(f"  → Raw rows: {len(df)}")
    
    # Rename columns
    rename_map = {
        "_id": "order_id",
        "customerId": "customer_id",
        "orderDate": "order_date",
        "orderStatus": "order_status",
        "orderCountry": "country",
        "orderState": "state",
        "orderEmail": "email",
        "orderFirstname": "first_name",
        "orderLastname": "last_name",
        "shopifyOrderId": "shopify_order_id",
        "product_name": "product_name",
        "sku": "sku",
        "variant_id": "variant_id",
        "variant_title": "variant_title",
        "title": "title",
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})
    
    # Parse datetime
    if 'order_date' in df.columns:
        df['order_date'] = parse_datetime_column(df['order_date'])
    
    # Lowercase email
    if 'email' in df.columns:
        df['email'] = df['email'].astype(str).str.lower().replace('nan', np.nan)
    
    # Remove null customer_id
    if 'customer_id' in df.columns:
        df = df.dropna(subset=['customer_id'])
    
    # Remove duplicates
    dedup_cols = ['order_id']
    if 'sku' in df.columns:
        dedup_cols.append('sku')
    if 'variant_id' in df.columns:
        dedup_cols.append('variant_id')
    existing_dedup = [c for c in dedup_cols if c in df.columns]
    if existing_dedup:
        df = df.drop_duplicates(subset=existing_dedup)
    
    # Select columns
    keep_cols = ['order_id', 'customer_id', 'email', 'order_date', 'order_status',
                 'country', 'state', 'product_name', 'sku', 'variant_id', 'variant_title',
                 'title', 'shopify_order_id', 'first_name', 'last_name']
    keep_cols = [c for c in keep_cols if c in df.columns]
    df = df[keep_cols]
    
    # Upload
    upload_csv_to_minio(df, "silver/orders_clean/data.csv", s3_client)
    print(f"  → Clean rows: {len(df)}")
    return df


def clean_customers(s3_client):
    """Clean customers data bằng Pandas"""
    print("\n📖 Reading customers from bronze...")
    
    key = get_latest_bronze_key(s3_client, "bronze/customers/")
    df = read_csv_from_minio(s3_client, key)
    print(f"  → Raw rows: {len(df)}")
    
    # Rename columns
    rename_map = {
        "_id": "customer_row_id",
        "customerId": "customer_id",
        "created": "created_at",
        "firstName": "first_name",
        "lastName": "last_name",
        "country": "country",
        "state": "state",
        "email": "email",
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})
    
    # Parse datetime
    if 'created_at' in df.columns:
        df['created_at'] = parse_datetime_column(df['created_at'])
    
    # Lowercase email
    if 'email' in df.columns:
        df['email'] = df['email'].astype(str).str.lower().replace('nan', np.nan)
    
    # Remove null customer_id
    if 'customer_id' in df.columns:
        df = df.dropna(subset=['customer_id'])
        df = df.drop_duplicates(subset=['customer_id'])
    
    # Drop sensitive columns
    drop_cols = ['password', 'vault_private', 'smileIoJWT', 'feed_private']
    df = df.drop(columns=[c for c in drop_cols if c in df.columns], errors='ignore')
    
    # Select columns
    keep_cols = ['customer_row_id', 'customer_id', 'email', 'first_name', 'last_name',
                 'country', 'state', 'created_at']
    keep_cols = [c for c in keep_cols if c in df.columns]
    df = df[keep_cols]
    
    # Upload
    upload_csv_to_minio(df, "silver/customers_clean/data.csv", s3_client)
    print(f"  → Clean rows: {len(df)}")
    return df


def main():
    print("=" * 60)
    print("🔄 BRONZE → SILVER TRANSFORMATION (Pandas)")
    print("=" * 60)
    
    s3_client = get_s3_client()
    
    clean_checkouts(s3_client)
    clean_orders(s3_client)
    clean_customers(s3_client)
    
    print("\n🎉 Silver layer complete!")


if __name__ == "__main__":
    main()
