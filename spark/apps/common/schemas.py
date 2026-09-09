"""
Centralised Schemas for AIS Maritime Data Processing
=====================================================
Single source of truth for raw Kafka payloads, HDFS historical Parquet
storage, and downstream batch analytical jobs.
"""

from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
    DateType,
)

# -----------------------------------------------------------------------------
# Raw Kafka JSON Schema (all incoming fields parsed as strings in PERMISSIVE mode)
# -----------------------------------------------------------------------------
AIS_RAW_SCHEMA = StructType([
    StructField("MMSI",             StringType(), True),
    StructField("BaseDateTime",     StringType(), True),
    StructField("LAT",              StringType(), True),
    StructField("LON",              StringType(), True),
    StructField("SOG",              StringType(), True),
    StructField("COG",              StringType(), True),
    StructField("Heading",          StringType(), True),
    StructField("VesselName",       StringType(), True),
    StructField("IMO",              StringType(), True),
    StructField("CallSign",         StringType(), True),
    StructField("VesselType",       StringType(), True),
    StructField("Status",           StringType(), True),
    StructField("Length",           StringType(), True),
    StructField("Width",            StringType(), True),
    StructField("Draft",            StringType(), True),
    StructField("Cargo",            StringType(), True),
    StructField("TransceiverClass", StringType(), True),
    StructField("_corrupt_record",  StringType(), True),
])

# -----------------------------------------------------------------------------
# Curated / Archival HDFS Parquet Schema (Cleaned & Typed)
# -----------------------------------------------------------------------------
AIS_HISTORICAL_SCHEMA = StructType([
    StructField("kafka_partition",  IntegerType(),   True),
    StructField("kafka_offset",     LongType(),      True),
    StructField("kafka_timestamp",  TimestampType(), True),
    StructField("MMSI",             LongType(),      False),
    StructField("BaseDateTime",     TimestampType(), False),
    StructField("LAT",              DoubleType(),    False),
    StructField("LON",              DoubleType(),    False),
    StructField("SOG",              DoubleType(),    True),
    StructField("COG",              DoubleType(),    True),
    StructField("Heading",          DoubleType(),    True),
    StructField("VesselName",       StringType(),    True),
    StructField("IMO",              StringType(),    True),
    StructField("CallSign",         StringType(),    True),
    StructField("VesselType",       IntegerType(),   True),
    StructField("Status",           IntegerType(),   True),
    StructField("Length",           DoubleType(),    True),
    StructField("Width",            DoubleType(),    True),
    StructField("Draft",            DoubleType(),    True),
    StructField("Cargo",            IntegerType(),   True),
    StructField("TransceiverClass", StringType(),    True),
    StructField("speed_kmh",        DoubleType(),    True),
    StructField("year",             IntegerType(),   True),
    StructField("month",            IntegerType(),   True),
    StructField("day",              IntegerType(),   True),
    StructField("hour",             IntegerType(),   True),
    StructField("day_of_week",      IntegerType(),   True),
    StructField("quarter",          IntegerType(),   True),
    StructField("date",             DateType(),      True),
])

# -----------------------------------------------------------------------------
# AIS Standard Vessel Type Code to Category Name Lookup
# (Standard IMO / USCG AIS Vessel Type codes)
# -----------------------------------------------------------------------------
VESSEL_TYPE_MAP = {
    0: "Not available / Default",
    20: "Wing in Ground",
    30: "Fishing",
    31: "Towing",
    32: "Towing: length exceeds 200m or breadth exceeds 25m",
    33: "Dredging or underwater ops",
    34: "Diving ops",
    35: "Military ops",
    36: "Sailing",
    37: "Pleasure Craft",
    40: "High Speed Craft (HSC)",
    50: "Pilot Vessel",
    51: "Search and Rescue vessel",
    52: "Tug",
    53: "Port Tender",
    54: "Anti-pollution equipment",
    55: "Law Enforcement",
    56: "Spare - Local Vessel",
    57: "Spare - Local Vessel",
    58: "Medical Transport",
    60: "Passenger",
    70: "Cargo",
    71: "Cargo - Hazard A",
    72: "Cargo - Hazard B",
    73: "Cargo - Hazard C",
    74: "Cargo - Hazard D",
    79: "Cargo - No additional info",
    80: "Tanker",
    81: "Tanker - Hazard A",
    82: "Tanker - Hazard B",
    83: "Tanker - Hazard C",
    84: "Tanker - Hazard D",
    89: "Tanker - No additional info",
    90: "Other Type",
}
