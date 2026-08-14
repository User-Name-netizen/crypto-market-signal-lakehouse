"""
Script để XÓA SẠCH checkpoint và delta log bị corrupt
Chạy script này TRƯỚC KHI chạy lại pyspark_stream_to_bronze.py
"""

import os

from pyspark.sql import SparkSession

print("🧹 BẮT ĐẦU DỌN DẸP CHECKPOINT BỊ CORRUPT...")

# Khởi tạo Spark với cấu hình mới (ĐÚNG VERSION)
spark = (SparkSession.builder
    .appName("Cleanup_Corrupt_Checkpoints")
    .config("spark.jars.packages", "io.delta:delta-spark_2.13:4.1.0,org.apache.hadoop:hadoop-aws:3.4.2,software.amazon.awssdk:bundle:2.29.52")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    
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

# Các đường dẫn cần dọn (CHỈ XÓA CHECKPOINT, KHÔNG CHẠM VÀO DELTA DATA)
checkpoint_stream_to_bronze = "s3a://lakehouse/checkpoints/stream_to_bronze"

# Hàm tiện ích để xóa thư mục trên MinIO qua Spark
def delete_hdfs_path(path_str, desc):
    print(f"\nĐang xóa {desc} tại: {path_str}")
    try:
        hadoop_conf = spark._jsc.hadoopConfiguration()
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
            spark._jvm.java.net.URI(path_str), hadoop_conf)
        path_obj = spark._jvm.org.apache.hadoop.fs.Path(path_str)
        
        if fs.exists(path_obj):
            fs.delete(path_obj, True)  # True = xóa đệ quy
            print(f"   ✅ Đã xóa thành công: {path_str}")
        else:
            print(f"   ℹ️ Thư mục không tồn tại (OK)")
    except Exception as e:
        print(f"   ⚠️ Lỗi khi xóa: {e}")

delete_hdfs_path(checkpoint_stream_to_bronze, "Checkpoint Kafka -> Bronze")

spark.stop()

print("\n🎉 HOÀN TẤT CHUẨN HÓA! ")
print("Dữ liệu Bronze và Silver vẫn được Tôn Trọng và Giữ Nguyên.")
print("Bây giờ bạn có thể mở lại các terminal và chạy tiếp các file:")
print("   - python processing/pyspark_stream_to_bronze.py")
print("   - python processing/pyspark_bronze_to_silver.py")
