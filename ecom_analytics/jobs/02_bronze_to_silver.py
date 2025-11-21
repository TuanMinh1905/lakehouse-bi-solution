# jobs/02_bronze_to_silver.py
import os
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql import types as T
from dotenv import load_dotenv

from spark_session import create_spark   # cùng folder jobs

BASE_DIR = os.path.dirname(os.path.dirname(__file__))
load_dotenv(os.path.join(BASE_DIR, "config", ".env"))

MINIO_BUCKET = os.getenv("MINIO_BUCKET")

def bronze_path(name: str):
    return f"s3a://{MINIO_BUCKET}/bronze/{name}/"

def silver_path(name: str):
    return f"s3a://{MINIO_BUCKET}/silver/{name}/"

def clean_checkouts(spark):
    df = spark.read.option("header", True).csv(bronze_path("checkouts"))

    # chuẩn hoá tên cột thành snake_case (ví dụ một số cột chính)
    df = (
        df
        .withColumnRenamed("_id", "checkout_id")
        .withColumnRenamed("customerId", "customer_id")
        .withColumnRenamed("createAt", "created_at")
        .withColumnRenamed("total", "total_amount")
        .withColumnRenamed("currency", "currency")
        .withColumnRenamed("channel", "channel")
    )

    # parse date
    df = df.withColumn("created_at", F.to_timestamp("created_at"))

    # bỏ những dòng không có customer_id
    df = df.filter(F.col("customer_id").isNotNull())

    # chuẩn hoá email lower-case
    if "email" in df.columns:
        df = df.withColumn("email", F.lower(F.col("email")))

    # bỏ cột không cần thiết (ví dụ):
    drop_cols = ["clientInfo", "token", "__v"]
    for c in drop_cols:
        if c in df.columns:
            df = df.drop(c)

    # bỏ duplicate
    df = df.dropDuplicates(["checkout_id"])

    df.write.mode("overwrite").parquet(silver_path("checkouts_clean"))
    print("Wrote Silver: checkouts_clean")

def clean_orders(spark):
    df = spark.read.option("header", True).csv(bronze_path("orders"))

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

    df = df.withColumn("order_date", F.to_timestamp("order_date"))

    df = df.filter(F.col("customer_id").isNotNull())

    # Nếu có cột quantity / price thì cast kiểu
    for col_name in ["quantity", "qty", "price"]:
        if col_name in df.columns:
            df = df.withColumn(col_name, F.col(col_name).cast(T.DoubleType()))

    df = df.dropDuplicates(["order_id", "sku", "variant_id"])

    df.write.mode("overwrite").parquet(silver_path("orders_clean"))
    print("Wrote Silver: orders_clean")

def clean_customers(spark):
    df = spark.read.option("header", True).csv(bronze_path("customers"))

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

    df = df.withColumn("created_at", F.to_timestamp("created_at"))
    df = df.withColumn("email", F.lower(F.col("email")))

    df = df.filter(F.col("customer_id").isNotNull())
    df = df.dropDuplicates(["customer_id"])

    drop_cols = ["password", "vault_private", "smileIoJWT"]
    for c in drop_cols:
        if c in df.columns:
            df = df.drop(c)

    df.write.mode("overwrite").parquet(silver_path("customers_clean"))
    print("Wrote Silver: customers_clean")

def main():
    spark = create_spark("BronzeToSilver")

    clean_checkouts(spark)
    clean_orders(spark)
    clean_customers(spark)

    spark.stop()

if __name__ == "__main__":
    main()
