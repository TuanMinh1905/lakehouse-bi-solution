# jobs/03_silver_to_gold.py
import os
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql import types as T
from dotenv import load_dotenv

from spark_session import create_spark

BASE_DIR = os.path.dirname(os.path.dirname(__file__))
load_dotenv(os.path.join(BASE_DIR, "config", ".env"))

MINIO_BUCKET = os.getenv("MINIO_BUCKET")

def silver_path(name: str):
    return f"s3a://{MINIO_BUCKET}/silver/{name}/"

def gold_path(name: str):
    return f"s3a://{MINIO_BUCKET}/gold/{name}/"

# ---------- DIMENSION TABLES ----------

def build_dim_customer(spark):
    customers = spark.read.parquet(silver_path("customers_clean"))

    dim_customer = (
        customers
        .select(
            "customer_id",
            "email",
            "first_name",
            "last_name",
            "country",
            "state",
            "created_at",
        )
        .dropDuplicates(["customer_id"])
    )

    dim_customer.write.mode("overwrite").parquet(gold_path("dim_customer"))
    print("Wrote Gold: dim_customer")

def build_dim_date(spark, orders, checkouts):
    # Lấy min/max date từ orders & checkouts
    order_minmax = orders.agg(F.min("order_date"), F.max("order_date")).collect()[0]
    checkout_minmax = checkouts.agg(F.min("created_at"), F.max("created_at")).collect()[0]

    min_date = min(order_minmax[0], checkout_minmax[0])
    max_date = max(order_minmax[1], checkout_minmax[1])

    df = spark.sql(f"""
        SELECT sequence(
            to_date('{min_date.strftime('%Y-%m-%d')}'),
            to_date('{max_date.strftime('%Y-%m-%d')}'),
            interval 1 day
        ) as date_seq
    """)

    dim_date = (
        df.select(F.explode("date_seq").alias("date"))
        .withColumn("date_key", F.date_format("date", "yyyyMMdd").cast(T.IntegerType()))
        .withColumn("day", F.dayofmonth("date"))
        .withColumn("month", F.month("date"))
        .withColumn("year", F.year("date"))
        .withColumn("week", F.weekofyear("date"))
        .withColumn("quarter", F.quarter("date"))
        .withColumn("is_weekend", F.col("date").isin(5, 6))
    )

    dim_date.write.mode("overwrite").parquet(gold_path("dim_date"))
    print("Wrote Gold: dim_date")

# ---------- FACT TABLES ----------

def build_fact_order(spark):
    orders = spark.read.parquet(silver_path("orders_clean"))

    fact_order = orders

    # Thêm cột ngày dạng key
    fact_order = fact_order.withColumn(
        "order_date_key",
        F.date_format("order_date", "yyyyMMdd").cast(T.IntegerType()),
    )

    # Nếu có quantity/price thì tính line_total (nếu chưa có)
    if "quantity" in fact_order.columns and "price" in fact_order.columns:
        fact_order = fact_order.withColumn(
            "line_total",
            F.col("quantity") * F.col("price")
        )

    fact_order.write.mode("overwrite").parquet(gold_path("fact_order"))
    print("Wrote Gold: fact_order")

def build_fact_checkout(spark):
    checkouts = spark.read.parquet(silver_path("checkouts_clean"))

    fact_checkout = checkouts.withColumn(
        "checkout_date_key",
        F.date_format("created_at", "yyyyMMdd").cast(T.IntegerType()),
    )

    fact_checkout.write.mode("overwrite").parquet(gold_path("fact_checkout"))
    print("Wrote Gold: fact_checkout")

# ---------- AGGREGATED TABLES (DATA MART) ----------

def build_agg_daily_sales(spark):
    fact_order = spark.read.parquet(gold_path("fact_order"))
    fact_checkout = spark.read.parquet(gold_path("fact_checkout"))

    orders_daily = (
        fact_order
        .groupBy(F.to_date("order_date").alias("date"))
        .agg(
            F.countDistinct("order_id").alias("total_orders"),
            F.sum("line_total").alias("total_revenue"),
        )
    )

    checkouts_daily = (
        fact_checkout
        .groupBy(F.to_date("created_at").alias("date"))
        .agg(F.countDistinct("checkout_id").alias("total_checkouts"))
    )

    agg = (
        orders_daily.join(checkouts_daily, "date", "full_outer")
        .na.fill(0, ["total_orders", "total_revenue", "total_checkouts"])
    )

    agg = agg.withColumn(
        "conversion_rate",
        F.when(F.col("total_checkouts") > 0,
               F.col("total_orders") / F.col("total_checkouts")).otherwise(0.0)
    )

    agg.write.mode("overwrite").parquet(gold_path("agg_daily_sales"))
    print("Wrote Gold: agg_daily_sales")

def build_agg_product_performance(spark):
    fact_order = spark.read.parquet(gold_path("fact_order"))

    agg = (
        fact_order
        .groupBy("sku", "product_name")
        .agg(
            F.sum("quantity").alias("units_sold"),
            F.countDistinct("order_id").alias("orders_count"),
            F.sum("line_total").alias("revenue"),
        )
    )

    agg.write.mode("overwrite").parquet(gold_path("agg_product_performance"))
    print("Wrote Gold: agg_product_performance")

def build_agg_customer_lifetime(spark):
    fact_order = spark.read.parquet(gold_path("fact_order"))

    agg = (
        fact_order
        .groupBy("customer_id")
        .agg(
            F.countDistinct("order_id").alias("orders_count"),
            F.sum("line_total").alias("lifetime_revenue"),
            F.min("order_date").alias("first_order_date"),
            F.max("order_date").alias("last_order_date"),
        )
    )

    agg.write.mode("overwrite").parquet(gold_path("agg_customer_lifetime"))
    print("Wrote Gold: agg_customer_lifetime")

# ---------- CUSTOMER SEGMENTATION (RFM) ----------

def rfm_score(col, quantiles, reverse=False):
    """
    Trả về biểu thức Spark tính score 1-5 dựa trên quantile cut.
    reverse=True dùng cho Recency (giá trị càng nhỏ càng tốt).
    """
    q1, q2, q3, q4 = quantiles
    if reverse:
        return (
            F.when(col <= q1, 5)
             .when(col <= q2, 4)
             .when(col <= q3, 3)
             .when(col <= q4, 2)
             .otherwise(1)
        )
    else:
        return (
            F.when(col <= q1, 1)
             .when(col <= q2, 2)
             .when(col <= q3, 3)
             .when(col <= q4, 4)
             .otherwise(5)
        )

def build_customer_segments(spark):
    agg = spark.read.parquet(gold_path("agg_customer_lifetime"))

    # Recency
    max_date = agg.agg(F.max("last_order_date")).collect()[0][0]
    agg = agg.withColumn(
        "recency_days",
        F.datediff(F.lit(max_date), F.col("last_order_date"))
    )

    # Tính quantile cho R, F, M
    qs = [0.2, 0.4, 0.6, 0.8]

    r_quantiles = agg.approxQuantile("recency_days", qs, 0.01)
    f_quantiles = agg.approxQuantile("orders_count", qs, 0.01)
    m_quantiles = agg.approxQuantile("lifetime_revenue", qs, 0.01)

    seg = (
        agg
        .withColumn("R_score", rfm_score(F.col("recency_days"), r_quantiles, reverse=True))
        .withColumn("F_score", rfm_score(F.col("orders_count"), f_quantiles))
        .withColumn("M_score", rfm_score(F.col("lifetime_revenue"), m_quantiles))
    )

    # Gán nhãn segment đơn giản
    seg = seg.withColumn(
        "segment_name",
        F.when((F.col("R_score") >= 4) & (F.col("F_score") >= 4) & (F.col("M_score") >= 4), "VIP")
         .when((F.col("F_score") >= 4) & (F.col("M_score") >= 3), "Loyal")
         .when((F.col("R_score") >= 4) & (F.col("F_score") <= 2), "New")
         .when((F.col("R_score") <= 2) & (F.col("F_score") >= 3), "At Risk")
         .otherwise("Other")
    )

    seg.write.mode("overwrite").parquet(gold_path("customer_segments"))
    print("Wrote Gold: customer_segments")

# ---------- MAIN ----------

def main():
    spark = create_spark("SilverToGold")

    orders_clean = spark.read.parquet(silver_path("orders_clean"))
    checkouts_clean = spark.read.parquet(silver_path("checkouts_clean"))

    # Dim / Fact
    build_dim_customer(spark)
    build_fact_order(spark)
    build_fact_checkout(spark)
    build_dim_date(spark, orders_clean, checkouts_clean)

    # Aggregations
    build_agg_daily_sales(spark)
    build_agg_product_performance(spark)
    build_agg_customer_lifetime(spark)

    # Segmentation
    build_customer_segments(spark)

    spark.stop()

if __name__ == "__main__":
    main()
