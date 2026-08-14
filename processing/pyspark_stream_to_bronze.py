import os
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StringType
from minio import Minio

# ================================================================
# SPARK STREAMING: Kafka → Bronze (Delta Lake trên MinIO)
# ================================================================

# Cấu hình chung cho Spark + MinIO + Delta Lake
SPARK_PACKAGES = (
    "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1,"
    "io.delta:delta-spark_2.13:4.1.0,"
    "org.apache.hadoop:hadoop-aws:3.4.2,"
    "software.amazon.awssdk:bundle:2.29.52"
)


def wait_for_minio(endpoint=None, max_retries=30, delay=5):
    """Đợi MinIO sẵn sàng trước khi tạo Spark session"""
    endpoint = endpoint or os.getenv("MINIO_ENDPOINT", "minio:9000")
    access_key = os.getenv("MINIO_ACCESS_KEY", "admin")
    secret_key = os.getenv("MINIO_SECRET_KEY")
    if not secret_key:
        raise ValueError("MINIO_SECRET_KEY must be set in the environment.")
    secure = os.getenv("MINIO_SECURE", "false").lower() == "true"
    for attempt in range(1, max_retries + 1):
        try:
            client = Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=secure)
            client.list_buckets()
            print(f"✅ MinIO sẵn sàng tại {endpoint}")
            return client
        except Exception as e:
            print(f"⏳ [{attempt}/{max_retries}] Đợi MinIO... ({e})")
            time.sleep(delay)
    raise ConnectionError(f"Không thể kết nối MinIO sau {max_retries} lần thử")


# 1. Đợi MinIO sẵn sàng
minio_client = wait_for_minio()
if not minio_client.bucket_exists("lakehouse"):
    minio_client.make_bucket("lakehouse")
    print("📦 Đã tạo bucket 'lakehouse'")

# 2. Khởi tạo Spark
print("🔧 Đang khởi tạo Spark Session...")
spark = (SparkSession.builder
    .appName("Lakehouse_Stream_To_Bronze")
    .config("spark.jars.packages", SPARK_PACKAGES)
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.parquet.enableVectorizedReader", "false")
    .config("spark.hadoop.fs.s3a.experimental.input.fadvise", "normal")
    .config("spark.hadoop.fs.s3a.connection.timeout", "60000")
    .config("spark.hadoop.fs.s3a.connection.establish.timeout", "60000")
    .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT_URL", "http://minio:9000"))
    .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY", "admin"))
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY"))
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate())

spark.sparkContext.setLogLevel("WARN")

# 3. Đọc luồng từ Kafka
df_kafka = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "binance_trades")
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load())

# Kafka trả về dữ liệu ở cột 'value' dạng Binary, ta chuyển sang String
df_stream = df_kafka.selectExpr("CAST(value AS STRING)")

# 4. Ghi luồng vào lớp BRONZE (Nạp chồng vào cùng folder với dữ liệu Batch)
bronze_path = "s3a://lakehouse/bronze/all_crypto_trades"
checkpoint_path = "s3a://lakehouse/checkpoints/stream_to_bronze"

query = (df_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path)
    .option("mergeSchema", "true")
    .start(bronze_path))

print(f"🟢 Đang bắt đầu luồng Streaming từ Kafka sang Bronze tại: {bronze_path}")
query.awaitTermination()
