import os
import time

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, expr, max, min, sum, when, window

# ================================================================
# SPARK STREAMING: Silver -> Gold (Tong hop analytics)
# ================================================================

SPARK_PACKAGES = (
    "io.delta:delta-spark_2.13:4.1.0,"
    "org.apache.hadoop:hadoop-aws:3.4.2,"
    "software.amazon.awssdk:bundle:2.29.52"
)

silver_path = "s3a://lakehouse/silver/btc_trades"
gold_ohlc_path = "s3a://lakehouse/gold/OHLC_1Min"
gold_whale_path = "s3a://lakehouse/gold/Whale_Alert"
gold_maker_taker_path = "s3a://lakehouse/gold/maker_taker_flow_1min"
gold_vwap_path = "s3a://lakehouse/gold/VWAP_1Min"
checkpoint_ohlc = "s3a://lakehouse/checkpoints/silver_to_gold_ohlc_1min"
checkpoint_whale = "s3a://lakehouse/checkpoints/silver_to_gold_whale_alert"
checkpoint_maker_taker = "s3a://lakehouse/checkpoints/silver_to_gold_maker_taker_flow_1min"
checkpoint_vwap = "s3a://lakehouse/checkpoints/silver_to_gold_vwap_1min"
whale_threshold_usdt = 50000.0


def wait_for_silver_table(spark_session, path, max_retries=90, delay=10):
    """Doi Silver Delta table ton tai truoc khi doc stream."""
    for attempt in range(1, max_retries + 1):
        try:
            hadoop_conf = spark_session._jsc.hadoopConfiguration()
            fs = spark_session._jvm.org.apache.hadoop.fs.FileSystem.get(
                spark_session._jvm.java.net.URI(path), hadoop_conf)
            delta_log = spark_session._jvm.org.apache.hadoop.fs.Path(path + "/_delta_log")
            if fs.exists(delta_log):
                print(f"Silver Delta table da san sang tai: {path}")
                return True
        except Exception as exc:
            print(f"[{attempt}/{max_retries}] Dang doi Silver table... ({exc})")
        time.sleep(delay)
    raise FileNotFoundError(f"Silver Delta table khong ton tai sau {max_retries * delay}s: {path}")


def is_delta_table(spark_session, path):
    try:
        return DeltaTable.isDeltaTable(spark_session, path)
    except Exception:
        return False


def merge_into_delta(spark_session, source_df, target_path, merge_condition, set_map, insert_map=None):
    if source_df.rdd.isEmpty():
        return

    if not is_delta_table(spark_session, target_path):
        (source_df.write
            .format("delta")
            .mode("append")
            .save(target_path))
        return

    delta_table = DeltaTable.forPath(spark_session, target_path)
    merge_builder = (delta_table.alias("target")
        .merge(source_df.alias("source"), merge_condition)
        .whenMatchedUpdate(set=set_map))

    if insert_map is None:
        merge_builder = merge_builder.whenNotMatchedInsertAll()
    else:
        merge_builder = merge_builder.whenNotMatchedInsert(values=insert_map)

    merge_builder.execute()


print("1. Dang khoi tao Spark Session cho pipeline Silver -> Gold...")
spark = (SparkSession.builder
    .appName("Lakehouse_Silver_To_Gold")
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

wait_for_silver_table(spark, silver_path)

print(f"2. Dang doc du lieu streaming tu lop Silver: {silver_path}")

df_silver = (spark.readStream
    .format("delta")
    .load(silver_path)
    .filter(
        col("event_id").isNotNull() &
        col("symbol").isNotNull() &
        col("price").isNotNull() &
        col("quantity").isNotNull() &
        col("quote_qty").isNotNull() &
        col("event_time").isNotNull()
    ))


# ============================================================
# GOLD TABLE 1: OHLC_1Min
# ============================================================
def upsert_ohlc_1min(micro_batch_df, batch_id):
    print(f"\n[Batch {batch_id}] Dang upsert incremental bang OHLC_1Min...")

    df_ohlc_batch = (micro_batch_df
        .groupBy(
            col("symbol"),
            window(col("event_time"), "1 minute").alias("time_window")
        )
        .agg(
            expr("min_by(price, event_time)").alias("open_price"),
            min("event_time").alias("open_event_time"),
            max("price").alias("high_price"),
            min("price").alias("low_price"),
            expr("max_by(price, event_time)").alias("close_price"),
            max("event_time").alias("close_event_time"),
            sum("quantity").alias("total_quantity"),
            sum("quote_qty").alias("total_quote_qty"),
            count("*").alias("total_trades"),
        )
        .select(
            col("symbol"),
            col("time_window.start").alias("candle_time"),
            col("open_price"),
            col("open_event_time"),
            col("high_price"),
            col("low_price"),
            col("close_price"),
            col("close_event_time"),
            col("total_quantity"),
            col("total_quote_qty"),
            col("total_trades"),
        ))

    merge_into_delta(
        spark_session=spark,
        source_df=df_ohlc_batch,
        target_path=gold_ohlc_path,
        merge_condition="""
            target.symbol = source.symbol
            AND target.candle_time = source.candle_time
        """,
        set_map={
            "open_price": """
                CASE
                    WHEN source.open_event_time < target.open_event_time THEN source.open_price
                    ELSE target.open_price
                END
            """,
            "open_event_time": "least(target.open_event_time, source.open_event_time)",
            "high_price": "greatest(target.high_price, source.high_price)",
            "low_price": "least(target.low_price, source.low_price)",
            "close_price": """
                CASE
                    WHEN source.close_event_time > target.close_event_time THEN source.close_price
                    ELSE target.close_price
                END
            """,
            "close_event_time": "greatest(target.close_event_time, source.close_event_time)",
            "total_quantity": "target.total_quantity + source.total_quantity",
            "total_quote_qty": "target.total_quote_qty + source.total_quote_qty",
            "total_trades": "target.total_trades + source.total_trades",
        }
    )


# ============================================================
# GOLD TABLE 2: Whale_Alert
# ============================================================
def append_whale_alert(micro_batch_df, batch_id):
    print(f"\n[Batch {batch_id}] Dang quet Whale Alert tu micro-batch...")

    whale_df = (micro_batch_df
        .filter(col("quote_qty") > whale_threshold_usdt)
        .withColumn("trade_value_usdt", col("quote_qty").cast("double"))
        .select(
            col("event_id"),
            col("symbol"),
            col("event_time"),
            col("price"),
            col("quantity"),
            col("quote_qty"),
            col("is_buyer_maker"),
            col("trade_value_usdt"),
        ))

    if whale_df.rdd.isEmpty():
        return

    merge_into_delta(
        spark_session=spark,
        source_df=whale_df.dropDuplicates(["event_id"]),
        target_path=gold_whale_path,
        merge_condition="target.event_id = source.event_id",
        set_map={
            "symbol": "target.symbol",
            "event_time": "target.event_time",
            "price": "target.price",
            "quantity": "target.quantity",
            "quote_qty": "target.quote_qty",
            "is_buyer_maker": "target.is_buyer_maker",
            "trade_value_usdt": "target.trade_value_usdt",
        }
    )
    print("  -> Da upsert Whale Alert theo event_id")


# ============================================================
# GOLD TABLE 3: Maker/Taker Flow
# ============================================================
def upsert_maker_taker_flow(micro_batch_df, batch_id):
    print(f"\n[Batch {batch_id}] Dang upsert incremental bang maker_taker_flow_1min...")

    df_maker_taker_batch = (micro_batch_df
        .filter(col("is_buyer_maker").isNotNull())
        .groupBy(
            col("symbol"),
            window(col("event_time"), "1 minute").alias("time_window")
        )
        .agg(
            sum(when(col("is_buyer_maker") == False, col("quantity")).otherwise(0)).alias("buy_aggressive_qty"),
            sum(when(col("is_buyer_maker") == True, col("quantity")).otherwise(0)).alias("sell_aggressive_qty"),
            sum(when(col("is_buyer_maker") == False, col("quote_qty")).otherwise(0)).alias("buy_aggressive_quote_qty"),
            sum(when(col("is_buyer_maker") == True, col("quote_qty")).otherwise(0)).alias("sell_aggressive_quote_qty"),
        )
        .select(
            col("symbol"),
            col("time_window.start").alias("window_start"),
            col("buy_aggressive_qty"),
            col("sell_aggressive_qty"),
            col("buy_aggressive_quote_qty"),
            col("sell_aggressive_quote_qty"),
            (col("buy_aggressive_quote_qty") - col("sell_aggressive_quote_qty")).alias("net_flow"),
        ))

    merge_into_delta(
        spark_session=spark,
        source_df=df_maker_taker_batch,
        target_path=gold_maker_taker_path,
        merge_condition="""
            target.symbol = source.symbol
            AND target.window_start = source.window_start
        """,
        set_map={
            "buy_aggressive_qty": "target.buy_aggressive_qty + source.buy_aggressive_qty",
            "sell_aggressive_qty": "target.sell_aggressive_qty + source.sell_aggressive_qty",
            "buy_aggressive_quote_qty": "target.buy_aggressive_quote_qty + source.buy_aggressive_quote_qty",
            "sell_aggressive_quote_qty": "target.sell_aggressive_quote_qty + source.sell_aggressive_quote_qty",
            "net_flow": "target.net_flow + source.net_flow",
        }
    )


# ============================================================
# GOLD TABLE 4: VWAP_1Min
# ============================================================
def upsert_vwap_1min(micro_batch_df, batch_id):
    print(f"\n[Batch {batch_id}] Dang upsert incremental bang VWAP_1Min...")

    df_vwap_batch = (micro_batch_df
        .groupBy(
            col("symbol"),
            window(col("event_time"), "1 minute").alias("time_window")
        )
        .agg(
            sum("quantity").alias("total_quantity"),
            sum("quote_qty").alias("total_quote_qty"),
            count("*").alias("trade_count"),
            expr("max_by(price, event_time)").alias("close_price"),
            max("event_time").alias("close_event_time"),
        )
        .select(
            col("symbol"),
            col("time_window.start").alias("window_start"),
            col("total_quantity"),
            col("total_quote_qty"),
            col("trade_count"),
            col("close_price"),
            col("close_event_time"),
        ))

    if df_vwap_batch.rdd.isEmpty():
        return

    if not is_delta_table(spark, gold_vwap_path):
        df_vwap_initial = df_vwap_batch.selectExpr(
            "symbol",
            "window_start",
            "total_quantity",
            "total_quote_qty",
            "trade_count",
            "close_price",
            "close_event_time",
            "total_quote_qty / total_quantity AS vwap_price",
            "total_quantity / trade_count AS avg_trade_size",
            "close_price - (total_quote_qty / total_quantity) AS close_vs_vwap_diff",
            "((close_price - (total_quote_qty / total_quantity)) / (total_quote_qty / total_quantity)) * 100 AS close_vs_vwap_pct",
        )
        (df_vwap_initial.write
            .format("delta")
            .mode("append")
            .save(gold_vwap_path))
        return

    delta_table = DeltaTable.forPath(spark, gold_vwap_path)
    (delta_table.alias("target")
        .merge(
            df_vwap_batch.alias("source"),
            """
                target.symbol = source.symbol
                AND target.window_start = source.window_start
            """
        )
        .whenMatchedUpdate(set={
            "total_quantity": "target.total_quantity + source.total_quantity",
            "total_quote_qty": "target.total_quote_qty + source.total_quote_qty",
            "trade_count": "target.trade_count + source.trade_count",
            "close_price": """
                CASE
                    WHEN source.close_event_time > target.close_event_time THEN source.close_price
                    ELSE target.close_price
                END
            """,
            "close_event_time": "greatest(target.close_event_time, source.close_event_time)",
            "vwap_price": """
                (target.total_quote_qty + source.total_quote_qty)
                / (target.total_quantity + source.total_quantity)
            """,
            "avg_trade_size": """
                (target.total_quantity + source.total_quantity)
                / (target.trade_count + source.trade_count)
            """,
            "close_vs_vwap_diff": """
                (
                    CASE
                        WHEN source.close_event_time > target.close_event_time THEN source.close_price
                        ELSE target.close_price
                    END
                ) - (
                    (target.total_quote_qty + source.total_quote_qty)
                    / (target.total_quantity + source.total_quantity)
                )
            """,
            "close_vs_vwap_pct": """
                (
                    (
                        CASE
                            WHEN source.close_event_time > target.close_event_time THEN source.close_price
                            ELSE target.close_price
                        END
                    ) - (
                        (target.total_quote_qty + source.total_quote_qty)
                        / (target.total_quantity + source.total_quantity)
                    )
                ) / (
                    (target.total_quote_qty + source.total_quote_qty)
                    / (target.total_quantity + source.total_quantity)
                ) * 100
            """,
        })
        .whenNotMatchedInsert(values={
            "symbol": "source.symbol",
            "window_start": "source.window_start",
            "total_quantity": "source.total_quantity",
            "total_quote_qty": "source.total_quote_qty",
            "trade_count": "source.trade_count",
            "close_price": "source.close_price",
            "close_event_time": "source.close_event_time",
            "vwap_price": "source.total_quote_qty / source.total_quantity",
            "avg_trade_size": "source.total_quantity / source.trade_count",
            "close_vs_vwap_diff": "source.close_price - (source.total_quote_qty / source.total_quantity)",
            "close_vs_vwap_pct": """
                ((source.close_price - (source.total_quote_qty / source.total_quantity))
                / (source.total_quote_qty / source.total_quantity)) * 100
            """,
        })
        .execute())


# ============================================================
# START ALL STREAMING QUERIES
# ============================================================

print("3. Dang tao bang Gold OHLC_1Min theo logic incremental merge...")
query_ohlc = (df_silver.writeStream
    .foreachBatch(upsert_ohlc_1min)
    .outputMode("append")
    .option("checkpointLocation", checkpoint_ohlc)
    .start())

print("4. Dang tao bang Gold Whale_Alert bang append...")
query_whale = (df_silver.writeStream
    .foreachBatch(append_whale_alert)
    .outputMode("append")
    .option("checkpointLocation", checkpoint_whale)
    .start())

print("5. Dang tao bang Gold maker_taker_flow_1min theo logic incremental merge...")
query_maker_taker = (df_silver.writeStream
    .foreachBatch(upsert_maker_taker_flow)
    .outputMode("append")
    .option("checkpointLocation", checkpoint_maker_taker)
    .start())

print("6. Dang tao bang Gold VWAP_1Min theo logic incremental merge...")
query_vwap = (df_silver.writeStream
    .foreachBatch(upsert_vwap_1min)
    .outputMode("append")
    .option("checkpointLocation", checkpoint_vwap)
    .start())

print(f"   -> Gold OHLC_1Min dang duoc cap nhat tai: {gold_ohlc_path}")
print(f"   -> Gold Whale_Alert dang duoc cap nhat tai: {gold_whale_path}")
print(f"   -> Gold maker_taker_flow_1min dang duoc cap nhat tai: {gold_maker_taker_path}")
print(f"   -> Gold VWAP_1Min dang duoc cap nhat tai: {gold_vwap_path}")
print(f"   -> Nguong canh bao giao dich lon: > {whale_threshold_usdt:,.0f} USDT/lenh")

spark.streams.awaitAnyTermination()
