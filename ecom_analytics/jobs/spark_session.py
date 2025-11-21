# jobs/spark_session.py
import os
from pyspark.sql import SparkSession
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", "config", ".env"))

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")

def create_spark(app_name: str = "EcomAnalytics"):
    """
    Tạo SparkSession đã cấu hình đọc/ghi MinIO qua s3a://
    """
    # Đường dẫn tới thư mục chứa JAR files
    jar_dir = r"C:\Users\GP\Desktop\VyVy\spark-jars"
    jars = [
        os.path.join(jar_dir, "hadoop-aws-3.3.4.jar"),
        os.path.join(jar_dir, "aws-java-sdk-bundle-1.12.262.jar")
    ]
    
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.jars", ",".join(jars))
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")  # nếu MinIO không dùng https
        .getOrCreate()
    )
    return spark
