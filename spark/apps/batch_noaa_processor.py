from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType


KAFKA_BOOTSTRAP = "kafka:9092"
KAFKA_TOPIC = "raw_ais_positions"

HDFS_OUTPUT = "hdfs://namenode:9000/processed/ais_history"


def main():

    print("=" * 80)
    print("AIS BATCH PROCESSOR")
    print("=" * 80)

    spark = (
        SparkSession.builder
        .appName("AIS Batch Processor")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    # ========================================================
    # AIS SCHEMA
    # ========================================================

    ais_schema = StructType([
        StructField("MMSI", StringType(), True),
        StructField("BaseDateTime", StringType(), True),
        StructField("LAT", StringType(), True),
        StructField("LON", StringType(), True),
        StructField("SOG", StringType(), True),
        StructField("COG", StringType(), True),
        StructField("Heading", StringType(), True),
        StructField("VesselName", StringType(), True),
        StructField("IMO", StringType(), True),
        StructField("CallSign", StringType(), True),
        StructField("VesselType", StringType(), True),
        StructField("Status", StringType(), True),
        StructField("Length", StringType(), True),
        StructField("Width", StringType(), True),
        StructField("Draft", StringType(), True),
        StructField("Cargo", StringType(), True),
        StructField("TransceiverClass", StringType(), True),
    ])

    # ========================================================
    # 1. READ DATA FROM KAFKA
    # ========================================================

    print("\n[1/10] Reading data from Kafka...")

    raw_df = (
        spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
    )

    raw_count = raw_df.count()

    print(f"Raw Kafka records: {raw_count}")

    # ========================================================
    # 2. PARSE JSON
    # ========================================================

    print("\n[2/10] Parsing JSON...")

    json_df = (
        raw_df
        .select(
            F.col("value")
            .cast("string")
            .alias("json")
        )
    )

    ais_df = (
        json_df
        .select(
            F.from_json(
                F.col("json"),
                ais_schema
            ).alias("data")
        )
        .select("data.*")
    )

    # ========================================================
    # 3. REMOVE REPEATED HEADERS + CLEAN STRINGS
    # ========================================================

    print("\n[3/10] Removing repeated headers and cleaning strings...")

    string_columns = [
        "MMSI",
        "VesselName",
        "IMO",
        "CallSign",
        "TransceiverClass"
    ]

    cleaned_df = ais_df

    for column in string_columns:

        cleaned_df = cleaned_df.withColumn(
            column,
            F.when(
                F.trim(F.col(column)) == "",
                None
            ).otherwise(
                F.trim(F.col(column))
            )
        )

    cleaned_df = (
        cleaned_df
        .filter(
            F.upper(F.trim(F.col("MMSI"))) != "MMSI"
        )
    )

    # ========================================================
    # 4. CONVERT DATA TYPES
    # ========================================================

    print("\n[4/10] Converting data types...")

    typed_df = (
        cleaned_df
        .withColumn("BaseDateTime", F.to_timestamp("BaseDateTime"))
        .withColumn("LAT", F.col("LAT").cast("double"))
        .withColumn("LON", F.col("LON").cast("double"))
        .withColumn("SOG", F.col("SOG").cast("double"))
        .withColumn("COG", F.col("COG").cast("double"))
        .withColumn("Heading", F.col("Heading").cast("double"))
        .withColumn("Length", F.col("Length").cast("double"))
        .withColumn("Width", F.col("Width").cast("double"))
        .withColumn("Draft", F.col("Draft").cast("double"))
        .withColumn("VesselType", F.col("VesselType").cast("integer"))
        .withColumn("Status", F.col("Status").cast("integer"))
        .withColumn("Cargo", F.col("Cargo").cast("integer"))
    )

    # ========================================================
    # 5. HANDLE AIS SPECIAL VALUES
    # ========================================================

    print("\n[5/10] Handling AIS special values...")

    special_values_df = (
        typed_df
        .withColumn(
            "Heading",
            F.when(
                F.col("Heading") == 511,
                None
            ).otherwise(F.col("Heading"))
        )
        .withColumn(
            "SOG",
            F.when(
                F.col("SOG") >= 102.2,
                None
            ).otherwise(F.col("SOG"))
        )
        .withColumn(
            "COG",
            F.when(
                F.col("COG") >= 360,
                None
            ).otherwise(F.col("COG"))
        )
        .withColumn(
            "IMO",
            F.when(
                F.upper(F.trim(F.col("IMO"))) == "IMO0000000",
                None
            ).otherwise(F.col("IMO"))
        )
    )

    # ========================================================
    # 6. VALIDATE TRACKING DATA
    # ========================================================

    print("\n[6/10] Validating tracking data...")

    valid_df = (
        special_values_df
        .filter(F.col("MMSI").rlike("^[0-9]{9}$"))
        .filter(F.col("BaseDateTime").isNotNull())
        .filter(F.col("LAT").isNotNull())
        .filter(F.col("LON").isNotNull())
        .filter(
            (F.col("LAT") >= -90) &
            (F.col("LAT") <= 90)
        )
        .filter(
            (F.col("LON") >= -180) &
            (F.col("LON") <= 180)
        )
        .filter(
            F.col("SOG").isNull() |
            (F.col("SOG") >= 0)
        )
        .filter(
            F.col("COG").isNull() |
            (
                (F.col("COG") >= 0) &
                (F.col("COG") < 360)
            )
        )
        .filter(
            F.col("Heading").isNull() |
            (
                (F.col("Heading") >= 0) &
                (F.col("Heading") < 360)
            )
        )
    )

    # ========================================================
    # 7. REMOVE DUPLICATES
    # ========================================================

    print("\n[7/10] Removing duplicate positions...")

    deduplicated_df = (
        valid_df
        .dropDuplicates([
            "MMSI",
            "BaseDateTime",
            "LAT",
            "LON"
        ])
    )

    # ========================================================
    # 8. RECOVER STABLE VESSEL INFORMATION
    # ========================================================

    print("\n[8/10] Recovering missing vessel information...")

    stable_columns = [
        "VesselName",
        "IMO",
        "CallSign",
        "VesselType",
        "Length",
        "Width",
        "Draft",
        "Cargo",
        "TransceiverClass"
    ]

    vessel_reference = (
        deduplicated_df
        .groupBy("MMSI")
        .agg(
            *[
                F.first(
                    F.col(column),
                    ignorenulls=True
                ).alias(
                    f"{column}_reference"
                )
                for column in stable_columns
            ]
        )
    )

    enriched_df = deduplicated_df.join(
        vessel_reference,
        on="MMSI",
        how="left"
    )

    for column in stable_columns:

        enriched_df = (
            enriched_df
            .withColumn(
                column,
                F.coalesce(
                    F.col(column),
                    F.col(f"{column}_reference")
                )
            )
            .drop(
                f"{column}_reference"
            )
        )

    # ========================================================
    # 9. CREATE ANALYTICAL FEATURES
    # ========================================================

    print("\n[9/10] Creating analytical features...")

    final_df = (
        enriched_df
        .withColumn(
            "speed_kmh",
            F.when(
                F.col("SOG").isNotNull(),
                F.round(
                    F.col("SOG") * F.lit(1.852),
                    2
                )
            )
        )
        .withColumn(
            "date",
            F.to_date("BaseDateTime")
        )
        .withColumn(
            "year",
            F.year("BaseDateTime")
        )
        .withColumn(
            "month",
            F.month("BaseDateTime")
        )
        .withColumn(
            "day",
            F.dayofmonth("BaseDateTime")
        )
        .withColumn(
            "hour",
            F.hour("BaseDateTime")
        )
        .withColumn(
            "day_of_week",
            F.dayofweek("BaseDateTime")
        )
        .withColumn(
            "quarter",
            F.quarter("BaseDateTime")
        )
    )

    # ========================================================
    # 10. FILL CATEGORICAL NULL VALUES
    # ========================================================

    categorical_columns = [
        "VesselName",
        "IMO",
        "CallSign",
        "TransceiverClass"
    ]

    for column in categorical_columns:

        final_df = final_df.withColumn(
            column,
            F.coalesce(
                F.col(column),
                F.lit("UNKNOWN")
            )
        )

    # ========================================================
    # FINAL COUNT
    # ========================================================

    final_count = final_df.count()

    print("\n" + "=" * 80)
    print("PROCESSING SUMMARY")
    print("=" * 80)

    print(f"Raw Kafka records:       {raw_count}")
    print(f"Processed records:       {final_count}")
    print(f"Removed records:         {raw_count - final_count}")

    # ========================================================
    # SHOW SAMPLE
    # ========================================================

    print("\nSample processed records:")

    final_df.select(
        "MMSI",
        "BaseDateTime",
        "LAT",
        "LON",
        "SOG",
        "speed_kmh",
        "COG",
        "Heading",
        "VesselName",
        "IMO",
        "VesselType",
        "Length",
        "Width",
        "Draft",
        "Cargo",
        "Status",
        "TransceiverClass",
        "date",
        "year",
        "month",
        "day",
        "hour",
        "day_of_week",
        "quarter"
    ).show(
        20,
        truncate=False
    )

    # ========================================================
    # WRITE TO HDFS
    # ========================================================

    print("\nWriting processed data to HDFS...")

    (
        final_df
        .repartition("date")
        .write
        .mode("overwrite")
        .partitionBy("date")
        .parquet(HDFS_OUTPUT)
    )

    # ========================================================
    # COMPLETED
    # ========================================================

    print("\n" + "=" * 80)
    print("AIS BATCH PROCESSING COMPLETED")
    print("=" * 80)

    print(f"HDFS output: {HDFS_OUTPUT}")

    spark.stop()


if __name__ == "__main__":
    main()