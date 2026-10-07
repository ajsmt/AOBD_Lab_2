from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json,
    col,
    window,
    session_window,
    count,
    avg,
    max,
    min,
    current_timestamp,
    lit,
    round,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType,
    TimestampType,
)

spark = SparkSession.builder.appName("Spark_Streaming_Full_Pipeline").getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

schema = StructType(
    [
        StructField("user_id", StringType(), True),
        StructField("action", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("timestamp_ms", LongType(), True),
        StructField("value", IntegerType(), True),
    ]
)

kafka_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "events-topic")
    .option("startingOffsets", "latest")
    .load()
)

events_df = (
    kafka_df.selectExpr("CAST(value AS STRING) as json_payload")
    .select(from_json(col("json_payload"), schema).alias("data"))
    .select("data.*")
)

watermarked_df = events_df.withWatermark("timestamp", "10 seconds")


tumbling_df = (
    watermarked_df.groupBy(window(col("timestamp"), "10 seconds"), col("action"))
    .agg(count("*").alias("count"))
    .select(
        lit("4b_TUMBLING").alias("type"),
        col("window.start"),
        col("window.end"),
        col("action"),
        col("count"),
    )
)

sliding_df = (
    watermarked_df.groupBy(
        window(col("timestamp"), "10 seconds", "5 seconds"), col("action")
    )
    .agg(count("*").alias("count"))
    .select(
        lit("4b_SLIDING").alias("type"),
        col("window.start"),
        col("window.end"),
        col("action"),
        col("count"),
    )
)

session_df = (
    watermarked_df.groupBy(
        session_window(col("timestamp"), "10 seconds"), col("user_id")
    )
    .agg(count("*").alias("session_count"))
    .select(
        lit("4b_SESSION").alias("type"),
        col("session_window.start"),
        col("session_window.end"),
        col("user_id"),
        col("session_count"),
    )
)

stateful_1min_df = (
    watermarked_df.groupBy(window(col("timestamp"), "1 minute"), col("user_id"))
    .agg(count("*").alias("events_per_minute"))
    .select(
        lit("4c_STATEFUL_1MIN").alias("task"),
        col("window.start"),
        col("window.end"),
        col("user_id"),
        col("events_per_minute"),
    )
)

metrics_df = (
    watermarked_df.withColumn(
        "latency_ms", (current_timestamp().cast("long") * 1000) - col("timestamp_ms")
    )
    .groupBy(window(col("timestamp"), "10 seconds"))
    .agg(
        count("*").alias("total_records"),
        round(avg("latency_ms"), 2).alias("avg_latency_ms"),
        min("latency_ms").alias("min_latency_ms"),
        max("latency_ms").alias("max_latency_ms"),
        round(count("*") / 10.0, 2).alias("throughput_rec_per_sec"),
    )
    .select(
        lit("4d_METRICS").alias("metric_report"),
        col("window.start"),
        col("window.end"),
        col("total_records"),
        col("avg_latency_ms"),
        col("throughput_rec_per_sec"),
    )
)

q_tumb = (
    tumbling_df.writeStream.outputMode("update")
    .format("console")
    .option("truncate", "false")
    .trigger(processingTime="10 seconds")
    .start()
)

q_slid = (
    sliding_df.writeStream.outputMode("update")
    .format("console")
    .option("truncate", "false")
    .trigger(processingTime="10 seconds")
    .start()
)

q_sess = (
    session_df.writeStream.outputMode("complete")
    .format("console")
    .option("truncate", "false")
    .trigger(processingTime="10 seconds")
    .start()
)

q_state = (
    stateful_1min_df.writeStream.outputMode("update")
    .format("console")
    .option("truncate", "false")
    .trigger(processingTime="10 seconds")
    .start()
)

q_metric = (
    metrics_df.writeStream.outputMode("update")
    .format("console")
    .option("truncate", "false")
    .trigger(processingTime="10 seconds")
    .start()
)

spark.streams.awaitAnyTermination()
