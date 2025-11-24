# jobs/spark_session.py
import os
import sys
from pyspark.sql import SparkSession
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", "config", ".env"))

def create_spark(app_name: str = "EcomAnalytics"):
    """
    Create SparkSession for local Parquet operations.
    No S3A configs needed - using local storage.
    """
    # For Windows, disable Hadoop checks that require winutils
    if sys.platform == "win32":
        # Create dummy hadoop home
        hadoop_home = r"C:\Users\GP\Desktop\VyVy\hadoop-tmp"
        bin_dir = os.path.join(hadoop_home, "bin")
        os.makedirs(bin_dir, exist_ok=True)
        os.environ["HADOOP_HOME"] = hadoop_home
        
        # Set HADOOP_OPTS to disable shell script invocations
        os.environ["HADOOP_OPTS"] = "-Dhadoop.home.dir=" + hadoop_home
    
    # Build SparkSession
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.sql.parquet.int96AsTimestamp", "false")
        .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
        .config("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")
        .config("spark.sql.parquet.int64RebaseModeInRead", "CORRECTED")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    
    spark.sparkContext.setLogLevel("ERROR")
    
    return spark
