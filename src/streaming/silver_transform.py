import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_timestamp,
    to_date,
    hour,
    when,
    round,
    unix_timestamp,
    count,
    sum,
    avg,
    max
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)

BRONZE_PATH = "data/bronze"
SILVER_PATH = "data/silver"

def build_spark() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("TaxiOps-Silver-Transform")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark

def main():
    print("starting")
    spark = build_spark()
    bronze_df = spark.read.parquet(BRONZE_PATH)

    silver_df = (
        bronze_df
        .withColumn("pickup_ts", to_timestamp("pickup_datetime"))
        .withColumn("dropoff_ts", to_timestamp("dropoff_datetime"))
        .filter(col("pickup_ts").isNotNull())
        .filter(col("dropoff_ts").isNotNull())
        .filter(col("fare_amount") > 0)
        .filter(col("trip_distance") > 0)
        .filter(col("dropoff_ts") > col("pickup_ts"))
        .withColumn(
            "trip_duration_min",
            round((unix_timestamp("dropoff_ts") - unix_timestamp("pickup_ts")) / 60.0, 2)
        )
    )

    silver_df.write.mode("overwrite").partitionBy("pickup_date").parquet(SILVER_PATH)

    silver_df.printSchema()
    silver_df.show(10, truncate=False)
    print("silver transformation finished")



if __name__ == "__main__":
    os.makedirs(SILVER_PATH, exist_ok=True)
    main()