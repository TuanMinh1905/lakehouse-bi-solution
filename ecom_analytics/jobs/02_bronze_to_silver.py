# jobs/02_bronze_to_silver.py
import os
import boto3
from dotenv import load_dotenv
from pyspark.sql import functions as F

from spark_session import create_spark

BASE_DIR = os.path.dirname(os.path.dirname(__file__))
load_dotenv(os.path.join(BASE_DIR, "config", ".env"))

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = os.getenv("MINIO_BUCKET")
MINIO_REGION = os.getenv("MINIO_REGION", "us-east-1")

LOCAL_BRONZE_DIR = r"C:\Users\GP\Desktop\VyVy\Data_Bronze"

def get_s3_client():
    """Tạo S3 client để ghi vào MinIO"""
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name=MINIO_REGION,
    )

def clean_checkouts(spark):
    """Clean checkouts data từ CSV (Spark) → CSV"""
    path = os.path.join(LOCAL_BRONZE_DIR, "checkouts.csv")
    print(f"Reading {path} ...")
    
    # Đọc CSV bằng Spark SQL
    df = spark.read.option("header", True).option("inferSchema", False).csv(path)
    
    # Rename columns to snake_case
    df = (
        df
        .withColumnRenamed("_id", "checkout_id")
        .withColumnRenamed("customerId", "customer_id")
        .withColumnRenamed("createAt", "created_at")
        .withColumnRenamed("total", "total_amount")
        .withColumnRenamed("currency", "currency")
        .withColumnRenamed("channel", "channel")
    )
    
    # Parse datetime
    df = df.withColumn("created_at", F.to_timestamp("created_at"))
    
    # Remove nulls
    df = df.filter(F.col("customer_id").isNotNull())
    
    # Lowercase email
    if "email" in df.columns:
        df = df.withColumn("email", F.lower(F.col("email")))
    
    # Drop unnecessary columns
    drop_cols = ["clientInfo", "token", "__v"]
    for c in drop_cols:
        if c in df.columns:
            df = df.drop(c)
    
    # Remove duplicates
    df = df.dropDuplicates(["checkout_id"])
    
    # Convert to CSV and upload
    csv_string = df.coalesce(1).toPandas().to_csv(index=False)
    
    s3_client = get_s3_client()
    s3_client.put_object(
        Bucket=MINIO_BUCKET,
        Key="silver/checkouts_clean/data.csv",
        Body=csv_string.encode('utf-8'),
        ContentType="text/csv",
    )
    print("Uploaded to s3://ecom-analytics/silver/checkouts_clean/data.csv")
    print("Wrote Silver: checkouts_clean")

def clean_orders(spark):
    """Clean orders data từ CSV (Spark) → CSV"""
    path = os.path.join(LOCAL_BRONZE_DIR, "orders.csv")
    print(f"Reading {path} ...")
    
    # Đọc CSV bằng Spark SQL
    df = spark.read.option("header", True).option("inferSchema", False).csv(path)
    
    # Rename columns
    df = (
        df
        .withColumnRenamed("_id", "order_id")
        .withColumnRenamed("customerId", "customer_id")
        .withColumnRenamed("orderDate", "order_date")
        .withColumnRenamed("orderStatus", "order_status")
        .withColumnRenamed("orderCountry", "country")
        .withColumnRenamed("product_name", "product_name")
        .withColumnRenamed("sku", "sku")
        .withColumnRenamed("variant_id", "variant_id")
        .withColumnRenamed("variant_title", "variant_title")
    )
    
    # Parse datetime
    df = df.withColumn("order_date", F.to_timestamp("order_date"))
    
    # Remove nulls
    df = df.filter(F.col("customer_id").isNotNull())
    
    # Cast numeric columns
    for col_name in ["quantity", "qty", "price"]:
        if col_name in df.columns:
            df = df.withColumn(col_name, F.col(col_name).cast("double"))
    
    # Remove duplicates
    df = df.dropDuplicates(["order_id", "sku", "variant_id"])
    
    # Convert to CSV and upload
    csv_string = df.coalesce(1).toPandas().to_csv(index=False)
    
    s3_client = get_s3_client()
    s3_client.put_object(
        Bucket=MINIO_BUCKET,
        Key="silver/orders_clean/data.csv",
        Body=csv_string.encode('utf-8'),
        ContentType="text/csv",
    )
    print("Uploaded to s3://ecom-analytics/silver/orders_clean/data.csv")
    print("Wrote Silver: orders_clean")

def clean_customers(spark):
    """Clean customers data từ CSV (Spark) → CSV"""
    path = os.path.join(LOCAL_BRONZE_DIR, "customers.csv")
    print(f"Reading {path} ...")
    
    # Đọc CSV bằng Spark SQL
    df = spark.read.option("header", True).option("inferSchema", False).csv(path)
    
    # Rename columns
    df = (
        df
        .withColumnRenamed("_id", "customer_row_id")
        .withColumnRenamed("customerId", "customer_id")
        .withColumnRenamed("created", "created_at")
        .withColumnRenamed("firstName", "first_name")
        .withColumnRenamed("lastName", "last_name")
        .withColumnRenamed("country", "country")
        .withColumnRenamed("state", "state")
    )
    
    # Parse datetime
    df = df.withColumn("created_at", F.to_timestamp("created_at"))
    
    # Lowercase email
    if "email" in df.columns:
        df = df.withColumn("email", F.lower(F.col("email")))
    
    # Remove nulls
    df = df.filter(F.col("customer_id").isNotNull())
    
    # Drop unnecessary columns
    drop_cols = ["password", "vault_private", "smileIoJWT"]
    for c in drop_cols:
        if c in df.columns:
            df = df.drop(c)
    
    # Remove duplicates
    df = df.dropDuplicates(["customer_id"])
    
    # Convert to CSV and upload
    csv_string = df.coalesce(1).toPandas().to_csv(index=False)
    
    s3_client = get_s3_client()
    s3_client.put_object(
        Bucket=MINIO_BUCKET,
        Key="silver/customers_clean/data.csv",
        Body=csv_string.encode('utf-8'),
        ContentType="text/csv",
    )
    print("Uploaded to s3://ecom-analytics/silver/customers_clean/data.csv")
    print("Wrote Silver: customers_clean")

def main():
    spark = create_spark("BronzeToSilver")
    
    print("Starting Bronze to Silver transformation...")
    clean_checkouts(spark)
    clean_orders(spark)
    clean_customers(spark)
    
    spark.stop()
    print("Done!")

if __name__ == "__main__":
    main()
