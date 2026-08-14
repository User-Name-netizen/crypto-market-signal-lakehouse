"""
Script de xoa du lieu cu cua cac lop Bronze, Silver, Gold va checkpoint lien quan.
Khong xoa lop Raw de tranh mat file CSV goc.
"""

import os

from pyspark.sql import SparkSession

print("Bat dau don dep du lieu cu cua Bronze, Silver, Gold...")

spark = (SparkSession.builder
    .appName("Clean_Old_Medallion_Data")
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

paths_to_delete = [
    ("Bronze data", "s3a://lakehouse/bronze/all_crypto_trades"),
    ("Silver data", "s3a://lakehouse/silver/btc_trades"),
    ("Gold OHLC_1Min", "s3a://lakehouse/gold/OHLC_1Min"),
    ("Gold Whale_Alert", "s3a://lakehouse/gold/Whale_Alert"),
    ("Gold maker_taker_flow_1min", "s3a://lakehouse/gold/maker_taker_flow_1min"),
    ("Gold VWAP_1Min", "s3a://lakehouse/gold/VWAP_1Min"),
    ("Checkpoint stream_to_bronze", "s3a://lakehouse/checkpoints/stream_to_bronze"),
    ("Checkpoint bronze_to_silver", "s3a://lakehouse/checkpoints/bronze_to_silver"),
    ("Checkpoint silver_to_gold_ohlc_1min", "s3a://lakehouse/checkpoints/silver_to_gold_ohlc_1min"),
    ("Checkpoint silver_to_gold_whale_alert", "s3a://lakehouse/checkpoints/silver_to_gold_whale_alert"),
    ("Checkpoint silver_to_gold_maker_taker_flow_1min", "s3a://lakehouse/checkpoints/silver_to_gold_maker_taker_flow_1min"),
    ("Checkpoint silver_to_gold_vwap_1min", "s3a://lakehouse/checkpoints/silver_to_gold_vwap_1min"),
]


def delete_hdfs_path(path_str, desc):
    print(f"\nDang xoa {desc}: {path_str}")
    try:
        hadoop_conf = spark._jsc.hadoopConfiguration()
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
            spark._jvm.java.net.URI(path_str), hadoop_conf)
        path_obj = spark._jvm.org.apache.hadoop.fs.Path(path_str)

        if fs.exists(path_obj):
            fs.delete(path_obj, True)
            print("   -> Da xoa thanh cong")
        else:
            print("   -> Khong ton tai, bo qua")
    except Exception as e:
        print(f"   -> Loi khi xoa: {e}")


for desc, path_str in paths_to_delete:
    delete_hdfs_path(path_str, desc)

spark.stop()

print("\nHoan tat don dep du lieu cu cua cac lop Bronze, Silver, Gold.")
print("Ban co the nap lai du lieu moi va chay lai pipeline tu dau.")
