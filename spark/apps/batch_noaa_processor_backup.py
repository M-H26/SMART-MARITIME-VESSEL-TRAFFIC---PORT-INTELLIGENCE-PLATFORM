"""
===============================================================================
Phase 1: Historical Archive Processing (Batch NOAA Processor)
===============================================================================

Required Task:
----------------
1. Read NOAA historical CSV data from HDFS (path: /raw/noaa/).
2. Clean and unify columns (MMSI, Timestamp, Latitude, Longitude, SOG, COG).
3. Handle nulls, duplicates, and validate geographic coordinates.
4. Calculate vessel speed in km/h and movement distances.
5. Save the result as Parquet format in HDFS under path (/processed/noaa_history).

Execution command via Spark Submit:
-----------------------------
spark-submit \
    --master spark://spark-master:7077 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.postgresql:postgresql:42.6.0 \
    /opt/spark-apps/batch_noaa_processor.py \
    --input  hdfs://namenode:9000/raw/noaa/sample.csv \
    --output hdfs://namenode:9000/processed/noaa_history \
    --run-date 2026-01-01
===============================================================================
"""
from __future__ import annotations

import argparse
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType,
)


def parse_arguments(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Batch NOAA AIS Processor")
    parser.add_argument("--input", required=True, help="HDFS input path (CSV file/dir)")
    parser.add_argument("--output", required=True, help="HDFS output path (Parquet)")
    parser.add_argument("--run-date", required=True, help="Processing run date (YYYY-MM-DD)")
    return parser.parse_args(argv)


def main() -> None:
    args = parse_arguments()

    print("==================================================================")
    print("🚀 [Batch NOAA Processor] Starting historical archive processing")
    print("👤 Responsible: Member 3 (Historical Archive - Batch Data)")
    print(f"📂 Input Path: {args.input}")
    print(f"💾 Output Path: {args.output}")
    print(f"📅 Run Date: {args.run_date}")
    print("==================================================================")

    # 1. Initialize Spark Session
    spark = (
        SparkSession.builder
        .appName(f"Batch NOAA Processor — {args.run_date}")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Define explicit schema for NOAA AIS data to prevent type errors
    schema = StructType([
        StructField("mmsi", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("lat", DoubleType(), True),
        StructField("lon", DoubleType(), True),
        StructField("sog", DoubleType(), True),
        StructField("cog", DoubleType(), True),
    ])

    # ------------------------------------------------------------------
    # Step 1 - Read CSV files from HDFS and apply the schema
    # ------------------------------------------------------------------
    raw_df = spark.read.option("header", "true").schema(schema).csv(args.input)

    # ------------------------------------------------------------------
    # Step 2 - Clean data (drop nulls, duplicates, and validate coordinates)
    # ------------------------------------------------------------------
    cleaned_df = (
        raw_df
        .dropna(subset=["mmsi", "lat", "lon", "sog"])
        .filter(
            (F.col("lat") >= -90.0) & (F.col("lat") <= 90.0) &
            (F.col("lon") >= -180.0) & (F.col("lon") <= 180.0) &
            (F.col("sog") >= 0.0)
        )
        .dropDuplicates(["mmsi", "timestamp"])
    )

    # ------------------------------------------------------------------
    # Step 3 - Calculate derived columns (convert speed from knots to km/h and set run_date)
    # ------------------------------------------------------------------
    processed_df = (
        cleaned_df
        .withColumn("speed_kmh", F.col("sog") * 1.852)
        .withColumn("run_date", F.lit(args.run_date))
    )

    # ------------------------------------------------------------------
    # Step 4 - Save processed data in Parquet format partitioned by date
    # ------------------------------------------------------------------
    (
        processed_df.write
        .mode("overwrite")
        .partitionBy("run_date")
        .parquet(args.output)
    )

    print("✅ Processing completed and data saved successfully to HDFS.")
    spark.stop()


if __name__ == "__main__":
    main()