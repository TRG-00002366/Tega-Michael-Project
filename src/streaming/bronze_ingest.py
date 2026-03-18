import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, to_date
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)

TOPIC = "nyc_taxi_trips"
BOOTSTRAP_SERVERS = "localhost:9092"

BRONZE_PATH = "data/bronze"
CHECKPOINT_PATH = "data/checkpoints/bronze"


def build_spark() -> SparkSession:
    packages = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8"

    spark = (
        SparkSession.builder
        .appName("TaxiOps-Bronze-Ingest")
        .master("local[*]")
        .config("spark.jars.packages", packages)
        .config("spark.sql.timestampFormat", "yyyy-MM-dd'T'HH:mm:ss[.SSSSSS]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def main():
    spark = build_spark()

    # Schema expected inside the Kafka JSON payload
    taxi_schema = StructType([
        StructField("event_id", StringType(), True),
        StructField("vendor_id", IntegerType(), True),
        StructField("pickup_datetime", StringType(), True),
        StructField("dropoff_datetime", StringType(), True),
        StructField("passenger_count", IntegerType(), True),
        StructField("trip_distance", DoubleType(), True),
        StructField("ratecode_id", IntegerType(), True),
        StructField("store_and_fwd_flag", StringType(), True),
        StructField("pu_location_id", IntegerType(), True),
        StructField("do_location_id", IntegerType(), True),
        StructField("payment_type", IntegerType(), True),
        StructField("fare_amount", DoubleType(), True),
        StructField("extra", DoubleType(), True),
        StructField("mta_tax", DoubleType(), True),
        StructField("tip_amount", DoubleType(), True),
        StructField("tolls_amount", DoubleType(), True),
        StructField("improvement_surcharge", DoubleType(), True),
        StructField("congestion_surcharge", DoubleType(), True),
        StructField("airport_fee", DoubleType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("event_timestamp", StringType(), True),
    ])

    # Read Kafka stream
    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "latest")
        .option("maxOffsetsPerTrigger", 500)
        .load()
    )

    # Parse Kafka JSON -> structured columns
    bronze_df = (
        kafka_df
        .select(col("value").cast("string").alias("json_str"))
        .select(from_json(col("json_str"), taxi_schema).alias("r"))
        .select("r.*")
        # Add partition column for the data lake
        .withColumn("pickup_ts", to_timestamp(col("pickup_datetime")))
        .withColumn("pickup_date", to_date(col("pickup_ts")))
        .drop("pickup_ts")
    )
    # Saves raw event to Bronze layer
    query = (
        bronze_df.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", BRONZE_PATH)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .partitionBy("pickup_date")
        .trigger(processingTime="1 minute")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    os.makedirs(BRONZE_PATH, exist_ok=True)
    os.makedirs(CHECKPOINT_PATH, exist_ok=True)
    main()