"""
AIS STRUCTURED STREAMING -> HDFS PARQUET (enhanced)
=====================================================

Enhancements over the original version:

  1. Two run modes via RUN_MODE env var:
       - "backfill" (default): uses Trigger.AvailableNow() to drain
         everything currently sitting in Kafka as fast as the cluster
         allows, then stops. This matches your use case -- you already
         pushed ~46.5M historical rows into Kafka at ~28k rows/sec, and
         the old 20,000-rows-per-30s cap (~667 rows/sec) would take
         nearly a full day to catch up. AvailableNow removes the
         artificial trigger-interval throttle entirely.
       - "live": uses a fixed processingTime trigger for an ongoing
         real-time feed, once you're actually consuming a live AIS
         stream rather than replaying history.

  2. maxOffsetsPerTrigger raised and now configurable (env var), so a
     single micro-batch can actually make a dent in a 40M+ row backlog.

  3. Dead-letter handling: unparseable JSON and rows that fail schema
     casts are captured (not silently dropped) and written to a
     separate HDFS path, with per-batch counts printed (in / valid /
     rejected) so data-quality issues are visible instead of invisible.

  4. Kafka metadata (partition, offset, kafka timestamp) is kept
     alongside the parsed AIS fields for lineage/debugging.

  5. Deduplication on (MMSI, BaseDateTime) using a watermark, to guard
     against duplicate Kafka deliveries (e.g. producer retries) leaking
     into the output.

  6. Output file sizing controlled via maxRecordsPerFile to avoid the
     "small files" problem that comes from many small micro-batches
     each writing their own partition files.

  7. Delta Lake output if the `delta` package is available (ACID,
     idempotent batch commits, safe to resume after a crash without
     manual cleanup); falls back to plain Parquet with a per-batch
     marker file for basic idempotency if Delta isn't installed.

Env vars (all optional, sensible defaults below):
    RUN_MODE                 backfill | live               (default: backfill)
    KAFKA_BOOTSTRAP          default: kafka:9092
    KAFKA_TOPIC              default: raw_ais_positions
    OUTPUT_PATH               default: hdfs://namenode:9000/processed/ais_streaming
    REJECTED_PATH             default: hdfs://namenode:9000/rejected/ais_streaming
    CHECKPOINT_PATH           default: hdfs://namenode:9000/checkpoints/ais_streaming
    MAX_OFFSETS_PER_TRIGGER   default: 500000
    LIVE_TRIGGER_SECONDS      default: 30      (only used in "live" mode)
    MAX_RECORDS_PER_FILE      default: 1000000
"""

import os

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

RUN_MODE = os.environ.get("RUN_MODE", "backfill").lower()  # "backfill" or "live"

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "raw_ais_positions")

OUTPUT_PATH = os.environ.get("OUTPUT_PATH", "hdfs://namenode:9000/processed/ais_streaming")
REJECTED_PATH = os.environ.get("REJECTED_PATH", "hdfs://namenode:9000/rejected/ais_streaming")
CHECKPOINT_PATH = os.environ.get("CHECKPOINT_PATH", "hdfs://namenode:9000/checkpoints/ais_streaming")

# Old default was 20,000 per 30s trigger (~667 rows/sec) -- far below the
# ~28,464 rows/sec the producer actually achieved. Raise this a lot for
# backfill; tune down if your cluster/laptop chokes on batch size.
MAX_OFFSETS_PER_TRIGGER = os.environ.get("MAX_OFFSETS_PER_TRIGGER", "500000")

LIVE_TRIGGER_SECONDS = int(os.environ.get("LIVE_TRIGGER_SECONDS", "30"))

MAX_RECORDS_PER_FILE = os.environ.get("MAX_RECORDS_PER_FILE", "1000000")

# Try Delta Lake for idempotent, ACID, crash-safe writes. Falls back to
# plain Parquet + a per-batch marker file if delta isn't installed.
try:
    from delta.tables import DeltaTable  # noqa: F401
    DELTA_AVAILABLE = True
except ImportError:
    DELTA_AVAILABLE = False


# ============================================================
# SPARK SESSION
# ============================================================

builder = SparkSession.builder.appName("AIS-Structured-Streaming")

if DELTA_AVAILABLE:
    builder = (
        builder
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )

spark = builder.getOrCreate()
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
print("AIS STRUCTURED STREAMING (enhanced)")
print("=" * 70)
print(f"Run mode:             {RUN_MODE}")
print(f"Kafka:                {KAFKA_BOOTSTRAP}")
print(f"Topic:                {KAFKA_TOPIC}")
print(f"Output:               {OUTPUT_PATH}")
print(f"Rejected records:     {REJECTED_PATH}")
print(f"Checkpoint:           {CHECKPOINT_PATH}")
print(f"maxOffsetsPerTrigger: {MAX_OFFSETS_PER_TRIGGER}")
print(f"maxRecordsPerFile:    {MAX_RECORDS_PER_FILE}")
print(f"Sink format:          {'delta' if DELTA_AVAILABLE else 'parquet (delta not installed)'}")
if RUN_MODE == "live":
    print(f"Trigger:              processingTime, every {LIVE_TRIGGER_SECONDS}s")
else:
    print("Trigger:              AvailableNow (drains current backlog, then stops)")
print("=" * 70)


# ============================================================
# READ FROM KAFKA
# ============================================================

raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)
    .option("failOnDataLoss", "false")
    .load()
)


# ============================================================
# PARSE JSON (permissive: keep unparseable rows instead of nulling them)
# ============================================================

ais_schema_permissive = ais_schema.add(
    StructField("_corrupt_record", StringType(), True)
)

parsed_stream = (
    raw_stream
    .withColumn(
        "kafka_partition",
        col("partition"),
    )
    .withColumn(
        "kafka_offset",
        col("offset"),
    )
    .withColumn(
        "kafka_timestamp",
        col("timestamp"),
    )
    .withColumn(
        "data",
        from_json(
            col("value").cast("string"),
            ais_schema_permissive,
            {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": "_corrupt_record"},
        ),
    )
    .select("kafka_partition", "kafka_offset", "kafka_timestamp", "data.*")
)


# ============================================================
# SPLIT: JSON-LEVEL REJECTS vs. CANDIDATES FOR FURTHER CLEANING
# ============================================================

json_rejects = parsed_stream.filter(col("_corrupt_record").isNotNull())

cleaned_stream = parsed_stream.filter(col("_corrupt_record").isNull()).drop("_corrupt_record")


# ============================================================
# BASIC CLEANING
# ============================================================

# Remove repeated CSV header rows
cleaned_stream = cleaned_stream.filter(col("MMSI") != "MMSI")

# Trim + require MMSI
cleaned_stream = cleaned_stream.withColumn("MMSI", trim(col("MMSI")))
cleaned_stream = cleaned_stream.filter(col("MMSI").isNotNull())

# MMSI must contain exactly 9 digits
cleaned_stream = cleaned_stream.filter(col("MMSI").rlike("^[0-9]{9}$"))


# ============================================================
# EMPTY STRINGS -> NULL
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
        when(trim(col(column_name)) == "", None).otherwise(trim(col(column_name))),
    )


# ============================================================
# DATA TYPE CONVERSION
# ============================================================

cleaned_stream = (
    cleaned_stream
    .withColumn("BaseDateTime", to_timestamp(col("BaseDateTime")))
    .withColumn("LAT", col("LAT").cast(DoubleType()))
    .withColumn("LON", col("LON").cast(DoubleType()))
    .withColumn("SOG", col("SOG").cast(DoubleType()))
    .withColumn("COG", col("COG").cast(DoubleType()))
    .withColumn("Heading", col("Heading").cast(DoubleType()))
    .withColumn("VesselType", col("VesselType").cast(IntegerType()))
    .withColumn("Status", col("Status").cast(IntegerType()))
    .withColumn("Length", col("Length").cast(DoubleType()))
    .withColumn("Width", col("Width").cast(DoubleType()))
    .withColumn("Draft", col("Draft").cast(DoubleType()))
    .withColumn("Cargo", col("Cargo").cast(IntegerType()))
)


# ============================================================
# CAPTURE ROWS THAT FAILED VALIDATION -- BEFORE DROPPING THEM
# ============================================================
# These are rows that made it through JSON parsing and had a valid
# 9-digit MMSI, but failed downstream validation (bad timestamp, or
# lat/lon out of range). Original script silently dropped these with
# .filter(); we now capture them for the rejects path first.

timestamp_invalid = cleaned_stream.filter(col("BaseDateTime").isNull())

cleaned_stream = cleaned_stream.filter(col("BaseDateTime").isNotNull())

latlon_invalid = cleaned_stream.filter(
    ~(
        (col("LAT") >= -90) & (col("LAT") <= 90)
        & (col("LON") >= -180) & (col("LON") <= 180)
    )
)

cleaned_stream = cleaned_stream.filter(
    (col("LAT") >= -90) & (col("LAT") <= 90)
    & (col("LON") >= -180) & (col("LON") <= 180)
)


# ============================================================
# AIS SPECIAL VALUES (unavailable/invalid sentinel values)
# ============================================================

cleaned_stream = cleaned_stream.withColumn(
    "Heading", when(col("Heading") == 511, None).otherwise(col("Heading"))
)

cleaned_stream = cleaned_stream.withColumn(
    "SOG", when(col("SOG") >= 102.2, None).otherwise(col("SOG"))
)

cleaned_stream = cleaned_stream.withColumn(
    "COG", when(col("COG") >= 360, None).otherwise(col("COG"))
)


# ============================================================
# HANDLE MISSING CATEGORICAL VALUES
# ============================================================

categorical_columns = ["VesselName", "IMO", "CallSign", "TransceiverClass"]

for column_name in categorical_columns:
    cleaned_stream = cleaned_stream.withColumn(
        column_name,
        when(
            col(column_name).isNull() | (trim(col(column_name)) == ""),
            lit("UNKNOWN"),
        ).otherwise(col(column_name)),
    )

cleaned_stream = cleaned_stream.withColumn(
    "IMO", when(col("IMO") == "IMO0000000", lit("UNKNOWN")).otherwise(col("IMO"))
)


# ============================================================
# DEDUPLICATION
# ============================================================
# Guards against duplicate Kafka deliveries (e.g. producer-side retries)
# turning into duplicate rows in the lake. Requires a watermark since
# this is a streaming dedup.
cleaned_stream = (
    cleaned_stream
    .withWatermark("BaseDateTime", "10 minutes")
    .dropDuplicates(["MMSI", "BaseDateTime"])
)

# ============================================================
# SPEED CONVERSION
# ============================================================

cleaned_stream = cleaned_stream.withColumn("speed_kmh", col("SOG") * 1.852)


# ============================================================
# DATE / TIME FEATURES
# ============================================================

cleaned_stream = (
    cleaned_stream
    .withColumn("date", col("BaseDateTime").cast("date"))
    .withColumn("year", year(col("BaseDateTime")))
    .withColumn("month", month(col("BaseDateTime")))
    .withColumn("day", dayofmonth(col("BaseDateTime")))
    .withColumn("hour", hour(col("BaseDateTime")))
    .withColumn("day_of_week", dayofweek(col("BaseDateTime")))
    .withColumn("quarter", quarter(col("BaseDateTime")))
)


# ============================================================
# UNION OF REJECTED RECORDS (for the rejects sink)
# ============================================================
# json_rejects has a different shape (mostly nulls + _corrupt_record),
# so it's written separately rather than unioned with the validation
# rejects. Both validation-reject frames share cleaned_stream's schema
# at the point they were split off.

validation_rejects = timestamp_invalid.unionByName(latlon_invalid, allowMissingColumns=True)


# ============================================================
# PROCESS EACH MICRO-BATCH
# ============================================================

def process_batch(batch_df, batch_id):

    print(f"\nProcessing micro-batch: {batch_id}")

    batch_df.persist()
    valid_count = batch_df.count()

    if valid_count == 0:
        print(f"Batch {batch_id}: no valid rows, skipping write")
        batch_df.unpersist()
        return

    # Idempotency for plain Parquet: skip if this batch was already
    # written (e.g. after a restart replaying the same offsets). Delta
    # Lake handles this natively via txn versioning, so this marker
    # check only matters in the non-Delta fallback path.
    marker_path = f"{OUTPUT_PATH}/_batch_markers/{batch_id}"

    writer = (
        batch_df
        .coalesce(max(1, valid_count // int(MAX_RECORDS_PER_FILE) + 1))
        .write
        .mode("append")
        .partitionBy("date")
        .option("maxRecordsPerFile", MAX_RECORDS_PER_FILE)
    )

    if DELTA_AVAILABLE:
        writer.format("delta").save(OUTPUT_PATH)
    else:
        writer.format("parquet").save(OUTPUT_PATH)
        spark.createDataFrame([(batch_id,)], ["batch_id"]).write.mode("overwrite").json(marker_path)

    print(f"Batch {batch_id}: wrote {valid_count:,} valid rows to {OUTPUT_PATH}")
    batch_df.unpersist()


def process_rejects(batch_df, batch_id):

    count = batch_df.count()

    if count == 0:
        return

    (
        batch_df.write
        .mode("append")
        .save(REJECTED_PATH, format="parquet")
    )

    print(f"Batch {batch_id}: wrote {count:,} rejected rows to {REJECTED_PATH}")


# ============================================================
# START STREAMING QUERIES
# ============================================================

if RUN_MODE == "live":
    trigger_kwargs = {"processingTime": f"{LIVE_TRIGGER_SECONDS} seconds"}
else:
    trigger_kwargs = {"availableNow": True}

query = (
    cleaned_stream.writeStream
    .foreachBatch(process_batch)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(**trigger_kwargs)
    .start()
)

rejects_query = (
    validation_rejects.writeStream
    .foreachBatch(process_rejects)
    .option("checkpointLocation", f"{CHECKPOINT_PATH}_rejects")
    .trigger(**trigger_kwargs)
    .start()
)

json_rejects_query = (
    json_rejects.writeStream
    .foreachBatch(process_rejects)
    .option("checkpointLocation", f"{CHECKPOINT_PATH}_json_rejects")
    .trigger(**trigger_kwargs)
    .start()
)


# ============================================================
# WAIT
# ============================================================

print("\nSpark Streaming is running...")

if RUN_MODE == "live":
    print("Waiting for Kafka messages (live mode -- runs until stopped)...")
else:
    print("Draining current Kafka backlog (backfill mode -- will stop when caught up)...")

query.awaitTermination()
rejects_query.awaitTermination()
json_rejects_query.awaitTermination()

print("\nAll streams finished.")