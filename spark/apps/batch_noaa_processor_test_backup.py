from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType


KAFKA_BOOTSTRAP = "kafka:9092"
KAFKA_TOPIC = "raw_ais_positions"


def main():

    print("=" * 70)
    print("AIS BATCH PROCESSOR")
    print("=" * 70)

    # Initialize Spark Session for batch processing
    spark = (
        SparkSession.builder
        .appName("AIS Batch Processor")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    # Define schema for the JSON messages coming from Kafka
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

    # Read existing messages from Kafka topic.
    # Starting offsets at earliest allows us to read the test data completely.
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

    # Convert Kafka binary value stream to a JSON string column
    json_df = raw_df.select(
        F.col("value").cast("string").alias("json")
    )

    # Parse JSON payload based on the defined schema
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

    # Clean, transform, and validate the parsed AIS dataframe
    cleaned_df = (
        ais_df

        # Cast BaseDateTime to proper timestamp format
        .withColumn(
            "BaseDateTime",
            F.to_timestamp("BaseDateTime")
        )

        # Cast attributes to appropriate numeric data types
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

        # Drop records lacking essential identification or location info
        .dropna(
            subset=[
                "MMSI",
                "BaseDateTime",
                "LAT",
                "LON"
            ]
        )

        # Validate geographic boundaries (latitude and longitude ranges)
        .filter(
            (F.col("LAT") >= -90) &
            (F.col("LAT") <= 90) &
            (F.col("LON") >= -180) &
            (F.col("LON") <= 180)
        )

        # Ensure speed over ground (SOG) is non-negative
        .filter(
            F.col("SOG").isNull() |
            (F.col("SOG") >= 0)
        )

        # Remove duplicate position entries for the same vessel at the exact timestamp
        .dropDuplicates(
            ["MMSI", "BaseDateTime", "LAT", "LON"]
        )

        # Derive new metric: convert speed from knots to kilometers per hour (km/h)
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