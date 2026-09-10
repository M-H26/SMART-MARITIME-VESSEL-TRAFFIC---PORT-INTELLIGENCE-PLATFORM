from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import StandardScaler, VectorAssembler  # <-- ضفنا StandardScaler
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import DoubleType
import math

spark = SparkSession.builder.appName("Maritime-KMeans-Anomaly").getOrCreate()

# قراءة الداتا المعالجة من HDFS
df = spark.read.parquet("hdfs://namenode:9000/processed/ais_history")

# 1. تجميع الأعمدة في Vector خام
assembler = VectorAssembler(
    inputCols=["lat", "lon", "sog", "cog"],
    outputCol="raw_features",
    handleInvalid="skip",
)
dataset = assembler.transform(df)

# 2. تطبيق الـ Z-Score Standardization (عشان نحل مشكلة تفاوت المقاييس)[cite: 1]
scaler = StandardScaler(
    inputCol="raw_features",
    outputCol="features",
    withStd=True,
    withMean=True,
)
scaler_model = scaler.fit(dataset)
scaled_dataset = scaler_model.transform(dataset)

# تدريب موديل K-Means بـ 5 مجموعات باستخدام الـ scaled features
kmeans = KMeans().setK(5).setSeed(42).setFeaturesCol("features")
model = kmeans.fit(scaled_dataset)
predictions = model.transform(scaled_dataset)

# استخراج مراكز المجموعات (Centroids)
centers = model.clusterCenters()


# دالة لحساب المسافة بين كل نقطة ومركز المجموعة بتاعتها
def calc_distance(features, cluster_id):
  center = centers[cluster_id]
  return float(math.sqrt(sum((f - c) ** 2 for f, c in zip(features, center))))


dist_udf = udf(calc_distance, DoubleType())
anomalies_df = predictions.withColumn(
    "distance", dist_udf(col("features"), col("prediction"))
)

# طباعة أكثر الحركات شذوذاً
print("Top 20 Anomalous Vessel Movements:")
anomalies_df.orderBy(col("distance").desc()).select(
    "mmsi", "lat", "lon", "sog", "prediction", "distance"
).show(20)

spark.stop()