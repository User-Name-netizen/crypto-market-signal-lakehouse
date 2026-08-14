import time

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce, col, concat_ws, from_json, from_unixtime, lit, lower, sha2, to_timestamp, when
from pyspark.sql.types import BooleanType, DoubleType, LongType, StringType, StructField, StructType

# ================================================================
# SPARK STREAMING: Bronze -> Silver (Lam sach, chuan hoa, dedupe)
# ================================================================

SPARK_PACKAGES = (
    "io.delta:delta-spark_2.13:4.1.0,"
    "org.apache.hadoop:hadoop-aws:3.4.2,"
    "software.amazon.awssdk:bundle:2.29.52"
)

bronze_path = "s3a://lakehouse/bronze/all_crypto_trades"
silver_path = "s3a://lakehouse/silver/btc_trades"
checkpoint_silver = "s3a://lakehouse/checkpoints/bronze_to_silver"


def wait_for_bronze_table(spark_session, path, max_retries=60, delay=10):
    """Doi Bronze Delta table ton tai truoc khi doc stream."""
    for attempt in range(1, max_retries + 1):
        try:
            hadoop_conf = spark_session._jsc.hadoopConfiguration()
            fs = spark_session._jvm.org.apache.hadoop.fs.FileSystem.get(
                spark_session._jvm.java.net.URI(path), hadoop_conf)
            delta_log = spark_session._jvm.org.apache.hadoop.fs.Path(path + "/_delta_log")
            if fs.exists(delta_log):
                print(f"Bronze Delta table da san sang tai: {path}")
                return True
        except Exception as exc:
            print(f"[{attempt}/{max_retries}] Dang doi Bronze table... ({exc})")
        time.sleep(delay)
    raise FileNotFoundError(f"Bronze Delta table khong ton tai sau {max_retries * delay}s: {path}")


def is_delta_table(spark_session, path):
    try:
        return DeltaTable.isDeltaTable(spark_session, path)
    except Exception:
        return False


import os

print("Dang khoi tao Spark Session cho pipeline Bronze -> Silver...")
spark = (SparkSession.builder
    .appName("Lakehouse_Bronze_To_Silver")
    .config("spark.jars.packages", SPARK_PACKAGES)
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.parquet.enableVectorizedReader", "false")
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

wait_for_bronze_table(spark, bronze_path)

json_schema = StructType([
    StructField("a", LongType(), True),
    StructField("p", StringType(), True),
    StructField("q", StringType(), True),
    StructField("T", LongType(), True),
    StructField("s", StringType(), True),
    StructField("m", BooleanType(), True)
])

df_bronze = spark.readStream.format("delta").load(bronze_path)

if "value" not in df_bronze.columns:
    df_bronze = df_bronze.withColumn("value", lit(None).cast("string"))

for batch_col in ["Trade_ID", "Price", "Quantity", "Quote_Qty", "Timestamp", "is_Buyer_Maker"]:
    if batch_col not in df_bronze.columns:
        df_bronze = df_bronze.withColumn(batch_col, lit(None).cast("string"))


df_silver = (df_bronze
    .withColumn("stream_data", when(col("value").isNotNull(), from_json(col("value"), json_schema)).otherwise(None))
    .withColumn("symbol", coalesce(col("stream_data.s"), lit("BTCUSDT")))
    .withColumn("price", coalesce(col("stream_data.p"), col("Price")).cast(DoubleType()))
    .withColumn("quantity", coalesce(col("stream_data.q"), col("Quantity")).cast(DoubleType()))
    .withColumn(
        "quote_qty",
        coalesce(
            col("Quote_Qty").cast(DoubleType()),
            (col("price") * col("quantity")).cast(DoubleType())
        )
    )
    .withColumn(
        "event_time",
        coalesce(
            to_timestamp(from_unixtime(col("stream_data.T").cast(DoubleType()) / 1000.0)),
            to_timestamp(from_unixtime(col("Timestamp").cast(DoubleType()) / 1000000.0))
        )
    )
    .withColumn(
        "is_buyer_maker",
        when(col("stream_data.m").isNotNull(), col("stream_data.m"))
        .when(lower(col("is_Buyer_Maker")) == "true", lit(True))
        .when(lower(col("is_Buyer_Maker")) == "false", lit(False))
        .otherwise(None)
        .cast(BooleanType())
    )
    .withColumn(
        "event_id",
        coalesce(
            when(
                col("stream_data.a").isNotNull(),
                concat_ws(":", lit("stream"), col("symbol"), col("stream_data.a").cast("string"))
            ),
            when(col("Trade_ID").isNotNull(), concat_ws(":", lit("batch"), col("Trade_ID"))),
            sha2(
                concat_ws(
                    "||",
                    col("symbol"),
                    col("event_time").cast("string"),
                    col("price").cast("string"),
                    col("quantity").cast("string"),
                    col("quote_qty").cast("string"),
                    col("is_buyer_maker").cast("string"),
                ),
                256
            )
        )
    )
    .select(
        col("event_id"),
        col("symbol"),
        col("price"),
        col("quantity"),
        col("quote_qty"),
        col("event_time"),
        col("is_buyer_maker")
    )
    .filter(
        col("event_id").isNotNull() &
        col("price").isNotNull() &
        (col("price") > 0) &
        col("quantity").isNotNull() &
        (col("quantity") > 0) &
        col("event_time").isNotNull()
    ))


def upsert_silver(micro_batch_df, batch_id):
    print(f"\n[Batch {batch_id}] Dang upsert idempotent vao Silver...")

    if micro_batch_df.rdd.isEmpty():
        return

    source_df = micro_batch_df.dropDuplicates(["event_id"])

    if not is_delta_table(spark, silver_path):
        (source_df.write
            .format("delta")
            .mode("append")
            .save(silver_path))
        return

    delta_table = DeltaTable.forPath(spark, silver_path)
    (delta_table.alias("target")
        .merge(
            source_df.alias("source"),
            "target.event_id = source.event_id"
        )
        .whenNotMatchedInsertAll()
        .execute())


query = (df_silver.writeStream
    .foreachBatch(upsert_silver)
    .outputMode("append")
    .option("checkpointLocation", checkpoint_silver)
    .start())

print(f"Lop Silver dang duoc tinh che va dedupe tai: {silver_path}")
query.awaitTermination()
