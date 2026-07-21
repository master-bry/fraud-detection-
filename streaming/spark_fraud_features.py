from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, window, count, avg, stddev, abs as spark_abs,
    when, lit, current_timestamp, udf
)
from pyspark.sql.types import DoubleType
import os

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
TOPIC_RAW = "transactions.raw"
TOPIC_SILVER = "transactions.silver"

POSTGRES_URL = "jdbc:postgresql://postgres:5432/fraud_detection"
POSTGRES_PROPS = {
    "user": "fraud_user",
    "password": "fraud_pass",
    "driver": "org.postgresql.Driver",
}


def create_spark_session():
    return (
        SparkSession.builder
        .appName("FraudDetectionStreaming")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.1")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def compute_velocity_features(df):
    window_1h = window(col("ingested_at"), "1 hour")
    window_24h = window(col("ingested_at"), "24 hours")

    velocity = (
        df.groupBy("nameOrig", window_1h)
        .agg(
            count("*").alias("velocity_1h"),
        )
    )

    velocity_24h = (
        df.groupBy("nameOrig", window_24h)
        .agg(
            count("*").alias("velocity_24h"),
        )
    )

    df_with_velocity = (
        df.join(velocity, on=["nameOrig", window_1h], how="left")
        .join(velocity_24h, on=["nameOrig", window_24h], how="left")
        .fillna(0, subset=["velocity_1h", "velocity_24h"])
    )

    return df_with_velocity


def compute_amount_zscore(df):
    stats = (
        df.groupBy("nameOrig")
        .agg(
            avg("amount").alias("avg_amount"),
            stddev("amount").alias("stddev_amount"),
        )
    )

    df_with_stats = df.join(stats, on="nameOrig", how="left")
    df_with_zscore = df_with_stats.withColumn(
        "amount_zscore",
        when(
            col("stddev_amount") > 0,
            (col("amount") - col("avg_amount")) / col("stddev_amount"),
        ).otherwise(0.0)
    )

    return df_with_zscore


def compute_balance_ratio(df):
    return df.withColumn(
        "balance_ratio_orig",
        when(
            col("oldbalanceOrg") > 0,
            col("amount") / col("oldbalanceOrg"),
        ).otherwise(0.0)
    )


def write_to_bronze(df, batch_id):
    if batch_id is not None and batch_id > 0:
        (
            df.write
            .format("jdbc")
            .option("url", POSTGRES_URL)
            .option("dbtable", "transactions_bronze")
            .options(**POSTGRES_PROPS)
            .mode("append")
            .save()
        )


def process_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    enriched = compute_balance_ratio(batch_df)
    enriched = compute_amount_zscore(enriched)

    enriched = enriched.withColumn("validated_at", current_timestamp())

    write_to_bronze(batch_df, batch_id)

    (
        enriched.write
        .format("jdbc")
        .option("url", POSTGRES_URL)
        .option("dbtable", "transactions_silver")
        .options(**POSTGRES_PROPS)
        .mode("append")
        .save()
    )


def main():
    spark = create_spark_session()

    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC_RAW)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    from pyspark.sql.functions import from_json
    from pyspark.sql.types import (
        StructType, StructField, StringType, FloatType
    )

    schema = StructType([
        StructField("step", FloatType()),
        StructField("type", StringType()),
        StructField("amount", FloatType()),
        StructField("nameOrig", StringType()),
        StructField("oldbalanceOrg", FloatType()),
        StructField("newbalanceOrig", FloatType()),
        StructField("nameDest", StringType()),
        StructField("oldbalanceDest", FloatType()),
        StructField("newbalanceDest", FloatType()),
        StructField("isFraud", FloatType()),
        StructField("isFlaggedFraud", FloatType()),
    ])

    parsed_stream = (
        raw_stream
        .selectExpr("CAST(value AS STRING)")
        .select(from_json(col("value"), schema).alias("data"))
        .select("data.*")
        .withColumn("ingested_at", current_timestamp())
    )

    query = (
        parsed_stream.writeStream
        .foreachBatch(process_batch)
        .outputMode("update")
        .option("checkpointLocation", "/opt/spark-data/checkpoints/transactions")
        .trigger(processingTime="10 seconds")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
