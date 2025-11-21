# jobs/01_upload_bronze.py
import os
import io
import pandas as pd
import boto3
from dotenv import load_dotenv
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.dirname(__file__))
load_dotenv(os.path.join(BASE_DIR, "config", ".env"))

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT").replace("http://", "").replace("https://", "")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = os.getenv("MINIO_BUCKET")
MINIO_REGION = os.getenv("MINIO_REGION", "us-east-1")

# Đường dẫn local tới 3 file Excel RAW của bạn
LOCAL_BRONZE_DIR = r"C:\Users\GP\Desktop\VyVy\Data_Bronze"   # TODO: sửa cho đúng

FILES = {
    "checkouts": "checkouts.csv",
    "orders": "orders.csv",
    "customers": "customers.csv",
}

def upload_csv_to_minio(df: pd.DataFrame, key: str, s3_client):
    buffer = io.StringIO()
    df.to_csv(buffer, index=False)
    buffer.seek(0)

    s3_client.put_object(
        Bucket=MINIO_BUCKET,
        Key=key,
        Body=buffer.getvalue().encode("utf-8"),
        ContentType="text/csv",
    )
    print(f"Uploaded to s3://{MINIO_BUCKET}/{key}")

def main():
    s3_client = boto3.client(
        "s3",
        endpoint_url=os.getenv("MINIO_ENDPOINT"),
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name=MINIO_REGION,
    )

    # Tạo bucket nếu chưa có
    existing = [b["Name"] for b in s3_client.list_buckets().get("Buckets", [])]
    if MINIO_BUCKET not in existing:
        s3_client.create_bucket(Bucket=MINIO_BUCKET)
        print(f"Created bucket: {MINIO_BUCKET}")

    today_str = datetime.today().strftime("%Y-%m-%d")

    for name, filename in FILES.items():
        path = os.path.join(LOCAL_BRONZE_DIR, filename)
        print(f"Reading {path} ...")
        try:
            df = pd.read_csv(path, on_bad_lines='skip', engine='python')
        except Exception as e:
            print(f"  Warning: {e}. Trying with error_bad_lines=False...")
            df = pd.read_csv(path, on_bad_lines='skip')

        key = f"bronze/{name}/{today_str}_{name}.csv"
        upload_csv_to_minio(df, key, s3_client)

if __name__ == "__main__":
    main()
