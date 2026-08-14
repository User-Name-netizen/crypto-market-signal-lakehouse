import os
import time

from minio import Minio
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

# ================================================================
# SPARK BATCH: CSV → Bronze (Delta Lake trên MinIO)
# ================================================================

print("1. Đang khởi tạo Spark Session và tải các thư viện kết nối MinIO, Delta Lake...")
print("   (Lưu ý: Lần chạy đầu tiên sẽ mất 1-3 phút để tải file .jar, vui lòng đợi!)")

SPARK_PACKAGES = (
    "io.delta:delta-spark_2.13:4.1.0,"
    "org.apache.hadoop:hadoop-aws:3.4.2,"
    "software.amazon.awssdk:bundle:2.29.52"
)

# Khởi tạo Spark với cấu hình thư viện (.jar) để nói chuyện với S3 (MinIO) và Delta
spark = (SparkSession.builder
    .appName("Lakehouse_Batch_Raw_To_Bronze")
    .config("spark.jars.packages", SPARK_PACKAGES)
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT_URL", "http://minio:9000"))
    .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY", "admin"))
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY"))
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate())

# Tắt bớt log rác của Spark cho dễ nhìn terminal
spark.sparkContext.setLogLevel("WARN")

# 2. Định nghĩa Schema dạng String (Giữ nguyên vẹn dữ liệu thô)
bronze_schema = StructType([
    StructField("Trade_ID", StringType(), True),
    StructField("Price", StringType(), True),
    StructField("Quantity", StringType(), True),
    StructField("Quote_Qty", StringType(), True),
    StructField("Timestamp", StringType(), True),
    StructField("is_Buyer_Maker", StringType(), True),
    StructField("is_Best_Match", StringType(), True)
])

# Batch đọc trực tiếp từ local mount (không qua lớp Raw trên MinIO)
primary_batch_dir = os.getenv("BATCH_DATA_DIR", "/app/infra/workspace")
fallback_batch_dir = "/app/infra/workspace"
batch_data_dir = primary_batch_dir if os.path.isdir(primary_batch_dir) else fallback_batch_dir
if not os.path.isdir(batch_data_dir):
    raise FileNotFoundError(
        f"Không tìm thấy thư mục dữ liệu batch: {primary_batch_dir} hoặc {fallback_batch_dir}"
    )

csv_files = sorted([f for f in os.listdir(batch_data_dir) if f.endswith(".csv")])
if not csv_files:
    raise FileNotFoundError(f"Không tìm thấy file CSV trong {batch_data_dir}")

raw_path = f"{batch_data_dir}/*.csv"
bronze_path = "s3a://lakehouse/bronze/all_crypto_trades"

# Đảm bảo bucket tồn tại trước khi ghi Delta
minio_client = Minio(
    os.getenv("MINIO_ENDPOINT", "minio:9000"),
    access_key=os.getenv("MINIO_ACCESS_KEY", "admin"),
    secret_key=os.getenv("MINIO_SECRET_KEY"),
    secure=os.getenv("MINIO_SECURE", "false").lower() == "true",
)
if not minio_client._provider and not os.getenv("MINIO_SECRET_KEY"):
    raise ValueError("MINIO_SECRET_KEY must be set in the environment.")
if not minio_client.bucket_exists("lakehouse"):
    minio_client.make_bucket("lakehouse")

print(f"\n2. Đang đọc dữ liệu batch trực tiếp từ local: {raw_path}")
# Đọc file CSV không có Header, ốp Schema vào
df_raw = spark.read.csv(raw_path, schema=bronze_schema, header=False)

print("   -> Xem thử 5 dòng dữ liệu chuẩn bị ghi xuống Bronze:")
df_raw.show(5)

print("3. Đang ghi dữ liệu xuống lớp BRONZE dưới định dạng Delta Lake...")
# Khởi tạo table nếu path chưa có để tránh conflict protocol với streaming writer
hadoop_conf = spark._jsc.hadoopConfiguration()
fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
    spark._jvm.java.net.URI(bronze_path), hadoop_conf
)
bronze_obj = spark._jvm.org.apache.hadoop.fs.Path(bronze_path)
if not fs.exists(bronze_obj):
    spark.createDataFrame([], bronze_schema).write.format("delta").mode("append").save(bronze_path)

# Ghi append để batch và streaming cùng hợp nhất vào Bronze
last_error = None
for attempt in range(1, 4):
    try:
        (df_raw.write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .save(bronze_path))
        last_error = None
        break
    except Exception as exc:
        last_error = exc
        message = str(exc)
        if "DELTA_PROTOCOL_CHANGED" in message or "ProtocolChangedException" in message:
            print(f"⚠️ Conflict Delta protocol (attempt {attempt}/3), retry sau 5s...")
            time.sleep(5)
            continue
        raise

if last_error is not None:
    raise last_error

print(f"\n✅ HOÀN TẤT! Dữ liệu Lô (Batch) đã được đưa vào Lakehouse thành công tại: {bronze_path}")

# Dừng Spark
spark.stop()
