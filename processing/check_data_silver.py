import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# 1. Khởi tạo Spark với cấu hình MinIO và Delta
spark = (SparkSession.builder
    .appName("Check_Silver_Data")
    .config("spark.jars.packages", "io.delta:delta-spark_2.13:4.1.0,org.apache.hadoop:hadoop-aws:3.4.2,software.amazon.awssdk:bundle:2.29.52")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT_URL", "http://minio:9000"))
    .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY", "admin"))
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY"))
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate())

# Tắt log rác
spark.sparkContext.setLogLevel("WARN")

# 2. Đường dẫn lớp Silver trên MinIO
silver_path = "s3a://lakehouse/silver/btc_trades"

print(f"Đang đọc dữ liệu Delta từ: {silver_path} ...\n")
try:
    # Đọc dữ liệu Delta
    df_silver = spark.read.format("delta").load(silver_path)
    
    # In ra cấu trúc (Schema)
    print("=== CẤU TRÚC BẢNG (SCHEMA) ===")
    df_silver.printSchema()
    
    # In ra 20 dòng dữ liệu mới nhất (sắp xếp theo thời gian sự kiện giảm dần)
    print("\n=== DỮ LIỆU BÊN TRONG (20 DÒNG MỚI NHẤT) ===")
    df_silver.orderBy(col("event_time").desc()).show(20, truncate=False)

    print("\n=== KIỂM TRA CÁC CỘT EDA MỚI ===")
    df_silver.select(
        "symbol",
        "price",
        "quantity",
        "quote_qty",
        "is_buyer_maker",
        "event_time"
    ).orderBy(col("event_time").desc()).show(20, truncate=False)
    
    # Đếm tổng số dòng hiện có
    total_rows = df_silver.count()
    print(f"\n=> 🎯 Tổng số dòng dữ liệu hiện có trong lớp Silver: {total_rows}")
    
except Exception as e:
    print(f"Có lỗi xảy ra (có thể thư mục chưa có dữ liệu): {e}")

spark.stop()