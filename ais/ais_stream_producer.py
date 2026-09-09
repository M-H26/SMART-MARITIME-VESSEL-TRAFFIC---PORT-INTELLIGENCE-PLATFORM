"""
HISTORICAL AIS -> KAFKA PRODUCER (fast version)
=================================================

Same behavior as the original script, but rewritten for throughput:

  1. One process per CSV file (up to CSV_WORKERS at a time), so all
     physical cores get used instead of just one.
  2. orjson instead of json for serialization (falls back to json if
     orjson isn't installed).
  3. csv.reader + zip() instead of csv.DictReader (avoids building an
     intermediate dict per row twice).
  4. No forced flush() after every file -- each worker flushes once,
     at the end of its own file, so files can pipeline instead of
     stalling on a full sync point.

Install the optional speed dependency:
    pip install orjson --break-system-packages

Usage is identical to the original -- same env vars:
    KAFKA_BOOTSTRAP, KAFKA_TOPIC, AIS_DATA_DIR, MAX_ROWS
plus one new one:
    CSV_WORKERS   (default: number of CPU cores, capped at number of files)
"""

import csv
import json
import multiprocessing as mp
import os
import time
from pathlib import Path

from kafka import KafkaProducer

try:
    import orjson
    def dumps(v):
        return orjson.dumps(v)
    JSON_LIB = "orjson"
except ImportError:
    def dumps(v):
        return json.dumps(v).encode("utf-8")
    JSON_LIB = "json (stdlib) -- install orjson for a big speedup"


# ============================================================
# Configuration
# ============================================================

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "raw_ais_positions")

AIS_DATA_DIR = Path(os.environ.get("AIS_DATA_DIR", "/data/historical"))

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

# How many files to process in parallel. Default: all cores, but never
# more than the number of files (more workers than files is wasted).
CSV_WORKERS = int(os.environ.get("CSV_WORKERS", str(min(len(CSV_FILES), os.cpu_count() or 4))))


# ============================================================
# Per-worker Kafka producer
# ============================================================
# NOTE: KafkaProducer instances are NOT fork-safe / picklable, so each
# worker process must create its own producer after it starts -- it
# cannot be created once at module level and shared like the original.

def make_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=dumps,
        key_serializer=lambda v: v.encode("utf-8") if v else None,
        batch_size=256 * 1024,
        linger_ms=20,
        buffer_memory=64 * 1024 * 1024,
        compression_type="lz4",
        max_in_flight_requests_per_connection=5,
    )


# ============================================================
# Process a single CSV file (runs inside a worker process)
# ============================================================

def process_file(filename):

    filepath = AIS_DATA_DIR / filename
    print(f"\nStarting: {filename}", flush=True)

    if not filepath.exists():
        print(f"File not found: {filepath}", flush=True)
        return filename, 0

    producer = make_producer()
    start_time = time.time()
    count = 0

    with open(
        filepath,
        "r",
        encoding="utf-8",
        errors="replace",
        newline="",
    ) as file:

        reader = csv.reader(file)

        try:
            header = next(reader)
        except StopIteration:
            producer.close()
            return filename, 0

        try:
            mmsi_idx = header.index("MMSI")
        except ValueError:
            print(f"{filename}: no MMSI column found, skipping", flush=True)
            producer.close()
            return filename, 0

        n_cols = len(header)

        for row in reader:

            # ------------------------------------------------
            # Remove repeated headers inside CSV
            # ------------------------------------------------
            if len(row) <= mmsi_idx:
                continue

            mmsi = row[mmsi_idx].strip()

            if not mmsi or mmsi == "MMSI":
                continue

            # ------------------------------------------------
            # Pad/truncate ragged rows to match header length,
            # then zip into a dict (same shape as the original
            # DictReader-based output).
            # ------------------------------------------------
            if len(row) < n_cols:
                row = row + [""] * (n_cols - len(row))
            elif len(row) > n_cols:
                row = row[:n_cols]

            data = {
                key: (value.strip() if value else value)
                for key, value in zip(header, row)
            }

            producer.send(KAFKA_TOPIC, key=mmsi, value=data)
            count += 1

            if MAX_ROWS > 0 and count >= MAX_ROWS:
                break

            if count % 100_000 == 0:
                elapsed = time.time() - start_time
                rate = count / elapsed if elapsed > 0 else 0
                print(
                    f"{filename}: {count:,} rows | {rate:,.0f} rows/sec",
                    flush=True,
                )

    producer.flush()
    producer.close()

    elapsed = time.time() - start_time
    rate = count / elapsed if elapsed > 0 else 0

    print(
        f"Finished: {filename} | rows={count:,} | rate={rate:,.0f} rows/sec",
        flush=True,
    )

    return filename, count


# ============================================================
# Main
# ============================================================

def main():

    print("=" * 60)
    print("HISTORICAL AIS -> KAFKA PRODUCER (fast)")
    print("=" * 60)
    print(f"Kafka: {KAFKA_BOOTSTRAP}")
    print(f"Topic: {KAFKA_TOPIC}")
    print(f"Data directory: {AIS_DATA_DIR}")
    print(f"MAX_ROWS per file: {MAX_ROWS}")
    print(f"JSON serializer: {JSON_LIB}")
    print(f"Workers: {CSV_WORKERS} (of {os.cpu_count()} CPUs detected)")
    print("=" * 60)

    total = 0
    overall_start = time.time()

    # One process per file, up to CSV_WORKERS at a time. Kafka brokers
    # handle many concurrent producers fine, so this is safe to run
    # against a single-broker dev cluster too -- just lower CSV_WORKERS
    # if you see the broker struggling.
    with mp.Pool(processes=CSV_WORKERS) as pool:
        for filename, count in pool.imap_unordered(process_file, CSV_FILES):
            total += count

    elapsed = time.time() - overall_start
    rate = total / elapsed if elapsed > 0 else 0

    print("\n" + "=" * 60)
    print(f"ALL FILES FINISHED | total={total:,} | rate={rate:,.0f} rows/sec")
    print("=" * 60)


if __name__ == "__main__":
    main()