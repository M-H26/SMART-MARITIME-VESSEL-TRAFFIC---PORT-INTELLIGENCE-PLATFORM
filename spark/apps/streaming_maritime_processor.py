from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    when,
    trim,
    lit,
    year,
    month,
    dayofmonth,
    hour,
    dayofweek,
    quarter,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
)


# ============================================================
# CONFIGURATION
# ============================================================

KAFKA_BOOTSTRAP = "kafka:9092"
KAFKA_TOPIC = "raw_ais_positions"

OUTPUT_PATH = "hdfs://namenode:9000/processed/ais_streaming"
CHECKPOINT_PATH = "hdfs://namenode:9000/checkpoints/ais_streaming"


# ============================================================
# SPARK SESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName("AIS-Structured-Streaming")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# AIS SCHEMA
# ============================================================

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


# ============================================================
# STARTUP INFORMATION
# ============================================================

print("=" * 70)
print("AIS STRUCTURED STREAMING")
print("=" * 70)
print(f"Kafka:       {KAFKA_BOOTSTRAP}")
print(f"Topic:       {KAFKA_TOPIC}")
print(f"Output:      {OUTPUT_PATH}")
print(f"Checkpoint:  {CHECKPOINT_PATH}")
print("Mode:        Lightweight streaming")
print("Dedup:       Disabled")
print("Max offsets: 20,000 records per micro-batch")
print("Trigger:     30 seconds")
print("=" * 70)


# ============================================================
# READ FROM KAFKA
# ============================================================

raw_stream = (
    spark.readStream
    .format("kafka")
    .option(
        "kafka.bootstrap.servers",
        KAFKA_BOOTSTRAP
    )
    .option(
        "subscribe",
        KAFKA_TOPIC
    )
    .option(
        "startingOffsets",
        "earliest"
    )
    .option(
        "maxOffsetsPerTrigger",
        "20000"
    )
    .option(
        "failOnDataLoss",
        "false"
    )
    .load()
)


# ============================================================
# PARSE JSON
# ============================================================

parsed_stream = (
    raw_stream
    .select(
        from_json(
            col("value").cast("string"),
            ais_schema
        ).alias("data")
    )
    .select("data.*")
)


# ============================================================
# BASIC CLEANING
# ============================================================

cleaned_stream = parsed_stream


# Remove repeated CSV header rows
cleaned_stream = cleaned_stream.filter(
    col("MMSI") != "MMSI"
)


# Trim MMSI
cleaned_stream = cleaned_stream.withColumn(
    "MMSI",
    trim(col("MMSI"))
)


# Remove missing MMSI
cleaned_stream = cleaned_stream.filter(
    col("MMSI").isNotNull()
)


# MMSI must contain exactly 9 digits
cleaned_stream = cleaned_stream.filter(
    col("MMSI").rlike("^[0-9]{9}$")
)


# ============================================================
# EMPTY STRINGS → NULL
# ============================================================

string_columns = [
    "BaseDateTime",
    "LAT",
    "LON",
    "SOG",
    "COG",
    "Heading",
    "VesselName",
    "IMO",
    "CallSign",
    "VesselType",
    "Status",
    "Length",
    "Width",
    "Draft",
    "Cargo",
    "TransceiverClass",
]


for column_name in string_columns:

    cleaned_stream = cleaned_stream.withColumn(
        column_name,
        when(
            trim(col(column_name)) == "",
            None
        ).otherwise(
            trim(col(column_name))
        )
    )


# ============================================================
# DATA TYPE CONVERSION
# ============================================================

cleaned_stream = (
    cleaned_stream

    # Timestamp
    .withColumn(
        "BaseDateTime",
        to_timestamp(col("BaseDateTime"))
    )

    # Position
    .withColumn(
        "LAT",
        col("LAT").cast(DoubleType())
    )
    .withColumn(
        "LON",
        col("LON").cast(DoubleType())
    )

    # Movement
    .withColumn(
        "SOG",
        col("SOG").cast(DoubleType())
    )
    .withColumn(
        "COG",
        col("COG").cast(DoubleType())
    )
    .withColumn(
        "Heading",
        col("Heading").cast(DoubleType())
    )

    # Vessel information
    .withColumn(
        "VesselType",
        col("VesselType").cast(IntegerType())
    )
    .withColumn(
        "Status",
        col("Status").cast(IntegerType())
    )
    .withColumn(
        "Length",
        col("Length").cast(DoubleType())
    )
    .withColumn(
        "Width",
        col("Width").cast(DoubleType())
    )
    .withColumn(
        "Draft",
        col("Draft").cast(DoubleType())
    )
    .withColumn(
        "Cargo",
        col("Cargo").cast(IntegerType())
    )
)


# ============================================================
# VALIDATE TIMESTAMP
# ============================================================

cleaned_stream = cleaned_stream.filter(
    col("BaseDateTime").isNotNull()
)


# ============================================================
# AIS SPECIAL VALUES
# ============================================================

# Heading = 511 means unavailable
cleaned_stream = cleaned_stream.withColumn(
    "Heading",
    when(
        col("Heading") == 511,
        None
    ).otherwise(
        col("Heading")
    )
)


# SOG >= 102.2 means unavailable/invalid
cleaned_stream = cleaned_stream.withColumn(
    "SOG",
    when(
        col("SOG") >= 102.2,
        None
    ).otherwise(
        col("SOG")
    )
)


# COG >= 360 means unavailable
cleaned_stream = cleaned_stream.withColumn(
    "COG",
    when(
        col("COG") >= 360,
        None
    ).otherwise(
        col("COG")
    )
)


# ============================================================
# VALIDATE LATITUDE / LONGITUDE
# ============================================================

cleaned_stream = cleaned_stream.filter(
    (col("LAT") >= -90) &
    (col("LAT") <= 90) &
    (col("LON") >= -180) &
    (col("LON") <= 180)
)


# ============================================================
# HANDLE MISSING CATEGORICAL VALUES
# ============================================================

categorical_columns = [
    "VesselName",
    "IMO",
    "CallSign",
    "TransceiverClass",
]


for column_name in categorical_columns:

    cleaned_stream = cleaned_stream.withColumn(
        column_name,
        when(
            col(column_name).isNull() |
            (trim(col(column_name)) == ""),
            lit("UNKNOWN")
        ).otherwise(
            col(column_name)
        )
    )


# ============================================================
# HANDLE INVALID IMO PLACEHOLDER
# ============================================================

cleaned_stream = cleaned_stream.withColumn(
    "IMO",
    when(
        col("IMO") == "IMO0000000",
        lit("UNKNOWN")
    ).otherwise(
        col("IMO")
    )
)


# ============================================================
# SPEED CONVERSION
# ============================================================

# SOG is in knots
# 1 knot = 1.852 km/h

cleaned_stream = cleaned_stream.withColumn(
    "speed_kmh",
    col("SOG") * 1.852
)


# ============================================================
# DATE / TIME FEATURES
# ============================================================

cleaned_stream = (
    cleaned_stream

    .withColumn(
        "date",
        col("BaseDateTime").cast("date")
    )

    .withColumn(
        "year",
        year(col("BaseDateTime"))
    )

    .withColumn(
        "month",
        month(col("BaseDateTime"))
    )

    .withColumn(
        "day",
        dayofmonth(col("BaseDateTime"))
    )

    .withColumn(
        "hour",
        hour(col("BaseDateTime"))
    )

    .withColumn(
        "day_of_week",
        dayofweek(col("BaseDateTime"))
    )

    .withColumn(
        "quarter",
        quarter(col("BaseDateTime"))
    )
)


# ============================================================
# PROCESS EACH MICRO-BATCH
# ============================================================

def process_batch(batch_df, batch_id):

    print(
        f"\nProcessing micro-batch: {batch_id}"
    )

    # --------------------------------------------------------
    # Write cleaned data directly to HDFS
    # --------------------------------------------------------

    (
        batch_df.write
        .mode("append")
        .partitionBy("date")
        .parquet(OUTPUT_PATH)
    )

    print(
        f"Batch {batch_id}: "
        f"written successfully to HDFS"
    )


# ============================================================
# START STREAMING QUERY
# ============================================================

query = (
    cleaned_stream.writeStream

    .foreachBatch(
        process_batch
    )

    .option(
        "checkpointLocation",
        CHECKPOINT_PATH
    )

    .trigger(
        processingTime="30 seconds"
    )

    .start()
)


# ============================================================
# WAIT
# ============================================================

print("\nSpark Streaming is running...")
print("Waiting for Kafka messages...")

query.awaitTermination()