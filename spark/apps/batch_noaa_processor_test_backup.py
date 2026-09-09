from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType


KAFKA_BOOTSTRAP = "kafka:9092"
KAFKA_TOPIC = "raw_ais_positions"


def main():

    print("=" * 70)
    print("AIS BATCH PROCESSOR")
    print("=" * 70)

    spark = (
        SparkSession.builder
        .appName("AIS Batch Processor")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    # Schema of the JSON messages coming from Kafka
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

    print("Reading AIS data from Kafka...")

    # Read the existing 1,000 messages from Kafka.
    # Starting offsets at earliest allows us to read the test data.
    raw_df = (
        spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
    )

    print(f"Kafka records read: {raw_df.count()}")

    # Kafka value is binary -> convert to JSON string
    json_df = raw_df.select(
        F.col("value").cast("string").alias("json")
    )

    # Parse JSON
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

    print("Converting data types...")

    cleaned_df = (
        ais_df

        # Timestamp
        .withColumn(
            "BaseDateTime",
            F.to_timestamp("BaseDateTime")
        )

        # Numeric fields
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

        # Remove records without essential information
        .dropna(
            subset=[
                "MMSI",
                "BaseDateTime",
                "LAT",
                "LON"
            ]
        )

        # Validate geographic coordinates
        .filter(
            (F.col("LAT") >= -90) &
            (F.col("LAT") <= 90) &
            (F.col("LON") >= -180) &
            (F.col("LON") <= 180)
        )

        # SOG cannot be negative
        .filter(
            F.col("SOG").isNull() |
            (F.col("SOG") >= 0)
        )

        # Remove duplicate AIS positions
        .dropDuplicates(
            ["MMSI", "BaseDateTime", "LAT", "LON"]
        )

        # Convert knots to km/h
        .withColumn(
            "speed_kmh",
            F.round(F.col("SOG") * F.lit(1.852), 2)
        )
    )

    print("=" * 70)
    print("CLEANED AIS DATA")
    print("=" * 70)

    print(f"Cleaned records: {cleaned_df.count()}")

    cleaned_df.printSchema()

    cleaned_df.show(
        20,
        truncate=False
    )

    print("=" * 70)
    print("AIS BATCH TEST COMPLETED")
    print("=" * 70)

    spark.stop()


if __name__ == "__main__":
    main()