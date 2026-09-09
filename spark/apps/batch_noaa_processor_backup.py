"""
===============================================================================
المرحلة الأولى: معالجة الأرشيف التاريخي (Batch NOAA Processor)
المسؤول: الفرد الثالث
===============================================================================

المهمة المطلوبة:
----------------
1. قراءة ملفات الـ CSV الخاصة ببيانات NOAA التاريخية من HDFS (مسار: /raw/noaa/).
2. تنظيف وتوحيد الأعمدة (MMSI, Timestamp, Latitude, Longitude, SOG, COG).
3. معالجة القيم الفارغة والتكرارات والتحقق من صحة الإحداثيات الجغرافية.
4. حساب سرعة السفن بوحدة كم/ساعة ومسافات التحرك.
5. حفظ النتيجة بصيغة Parquet في HDFS تحت المسار (/processed/noaa_history).

أمر التشغيل عبر Spark Submit:
-----------------------------
spark-submit \\
    --master spark://spark-master:7077 \\
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.postgresql:postgresql:42.6.0 \\
    /opt/spark-apps/batch_noaa_processor.py \\
    --input  hdfs://namenode:9000/raw/noaa/sample.csv \\
    --output hdfs://namenode:9000/processed/noaa_history \\
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
    print("🚀 [Batch NOAA Processor] بدء معالجة بيانات الأرشيف التاريخي")
    print("👤 المسؤول: الفرد الثالث (الأرشيف التاريخي - Batch Data)")
    print(f"📂 المسار المدخل: {args.input}")
    print(f"💾 المسار المخرج: {args.output}")
    print("==================================================================")

    # 1. تهيئة جلسة السبارك (SparkSession)
    spark = (
        SparkSession.builder
        .appName(f"Batch NOAA Processor — {args.run_date}")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # ------------------------------------------------------------------
    # TODO: الخطوة 1 - قراءة ملفات CSV من HDFS وتطبيق Schema مناسبة
    # ------------------------------------------------------------------
    # raw_df = spark.read.option("header", "true").csv(args.input)

    # ------------------------------------------------------------------
    # TODO: الخطوة 2 - تنظيف البيانات (Drop Nulls, Filter Valid Coordinates)
    # ------------------------------------------------------------------

    # ------------------------------------------------------------------
    # TODO: الخطوة 3 - حساب الأعمدة المشتقة (Derived Metrics like speed_kmh)
    # ------------------------------------------------------------------

    # ------------------------------------------------------------------
    # TODO: الخطوة 4 - حفظ البيانات المعالجة بصيغة Parquet مقسمة حسب التاريخ
    # ------------------------------------------------------------------
    # cleaned_df.write.mode("overwrite").partitionBy("run_date").parquet(args.output)

    print("✅ اكتملت المعالجة بنجاح.")
    spark.stop()


if __name__ == "__main__":
    main()
