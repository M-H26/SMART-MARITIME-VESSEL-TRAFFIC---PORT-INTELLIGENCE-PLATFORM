import csv
import json
import os
import time
from pathlib import Path

from kafka import KafkaProducer


# ============================================================
# Configuration
# ============================================================

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "raw_ais_positions")

AIS_DATA_DIR = Path(
    os.environ.get("AIS_DATA_DIR", "/data/historical")
)

MAX_ROWS = int(os.environ.get("MAX_ROWS", "0"))

CSV_FILES = [
    "AIS_2024_12_25.csv",
    "AIS_2024_12_26.csv",
    "AIS_2024_12_27.csv",
    "AIS_2024_12_28.csv",
    "AIS_2024_12_29.csv",
    "AIS_2024_12_30.csv",
    "AIS_2024_12_31.csv",
]


# ============================================================
# Kafka Producer
# ============================================================

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,

    # JSON serialization
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda v: v.encode("utf-8") if v else None,

    # Performance settings
    batch_size=256 * 1024,
    linger_ms=20,
    buffer_memory=64 * 1024 * 1024,

    # Compression
    compression_type="lz4",

    # Allow multiple requests in flight
    max_in_flight_requests_per_connection=5,
)


# ============================================================
# Process CSV files
# ============================================================

def process_file(filename):

    filepath = AIS_DATA_DIR / filename

    print(f"\nStarting: {filename}")

    if not filepath.exists():
        print(f"File not found: {filepath}")
        return 0

    start_time = time.time()
    count = 0

    with open(
        filepath,
        "r",
        encoding="utf-8",
        errors="replace",
        newline=""
    ) as file:

        reader = csv.DictReader(file)

        for row in reader:

            # ------------------------------------------------
            # Remove repeated headers inside CSV
            # ------------------------------------------------

            if row.get("MMSI") == "MMSI":
                continue

            mmsi = row.get("MMSI", "").strip()

            if not mmsi:
                continue

            # ------------------------------------------------
            # Convert row to normal dictionary
            # ------------------------------------------------

            data = {
                key: value.strip() if isinstance(value, str) else value
                for key, value in row.items()
            }

            # ------------------------------------------------
            # Send to Kafka
            # ------------------------------------------------

            producer.send(
                KAFKA_TOPIC,
                key=mmsi,
                value=data
            )

            count += 1

            # ------------------------------------------------
            # MAX_ROWS is per file
            # 0 = no limit
            # ------------------------------------------------

            if MAX_ROWS > 0 and count >= MAX_ROWS:
                break

            # ------------------------------------------------
            # Progress
            # ------------------------------------------------

            if count % 100_000 == 0:

                elapsed = time.time() - start_time
                rate = count / elapsed if elapsed > 0 else 0

                print(
                    f"{filename}: "
                    f"{count:,} rows | "
                    f"{rate:,.0f} rows/sec"
                )

    # --------------------------------------------------------
    # Make sure this file's messages are sent
    # --------------------------------------------------------

    producer.flush()

    elapsed = time.time() - start_time
    rate = count / elapsed if elapsed > 0 else 0

    print(
        f"Finished: {filename} | "
        f"rows={count:,} | "
        f"rate={rate:,.0f} rows/sec"
    )

    return count


# ============================================================
# Main
# ============================================================

def main():

    print("=" * 60)
    print("HISTORICAL AIS → KAFKA PRODUCER")
    print("=" * 60)

    print(f"Kafka: {KAFKA_BOOTSTRAP}")
    print(f"Topic: {KAFKA_TOPIC}")
    print(f"Data directory: {AIS_DATA_DIR}")
    print(f"MAX_ROWS per file: {MAX_ROWS}")
    print("Workers: 1")
    print("=" * 60)

    total = 0
    overall_start = time.time()

    try:

        for filename in CSV_FILES:

            total += process_file(filename)

    finally:

        producer.flush()
        producer.close()

    elapsed = time.time() - overall_start
    rate = total / elapsed if elapsed > 0 else 0

    print("\n" + "=" * 60)
    print(
        f"ALL FILES FINISHED | "
        f"total={total:,} | "
        f"rate={rate:,.0f} rows/sec"
    )
    print("=" * 60)


if __name__ == "__main__":
    main()