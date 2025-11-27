from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, expr
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"E:\Usuarios\Personal\Downloads\DanielDownloads\bigdata-479408-550fd68cbef5.json"


# ============================
# CONFIG
# ============================

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "music_stream"

TEMP_GCS_PATH = "gs://bucket-bigdata-music2/kafka_streaming/"  # ruta temporal en GCS
CHECKPOINT = "checkpoint_kafka_gcs/"

# ============================
# SCHEMA
# ============================

schema = StructType([
    StructField("event_id", StringType()),
    StructField("user_id", IntegerType()),
    StructField("country", StringType()),
    StructField("name", StringType()),
    StructField("artist_name", StringType()),
    StructField("playcount", IntegerType()),
    StructField("rank", IntegerType()),
    StructField("type", StringType()),
    StructField("timestamp", TimestampType()),
])

# ============================
# SPARK SESSION
# ============================

spark = SparkSession.builder \
    .appName("KafkaToGCS_Final") \
    .config(
        "spark.jars.packages",
        ",".join([
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7",  # Kafka
            "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.10"  # GCS
        ])
    ) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ============================
# LEER KAFKA
# ============================

df_raw = spark.readStream \
   .format("kafka") \
   .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
   .option("subscribe", TOPIC) \
   .option("startingOffsets", "earliest") \
   .load()

df_parsed = df_raw.select(
    from_json(expr("CAST(value AS STRING)"), schema).alias("data")
).select("data.*")

df_final = df_parsed.withColumn("event_date", expr("DATE(timestamp)"))

# ============================
# GUARDAR EN GCS (STAGING)
# ============================

query = df_final.writeStream \
    .format("parquet") \
    .option("path", TEMP_GCS_PATH) \
    .option("checkpointLocation", CHECKPOINT) \
    .outputMode("append") \
    .start()

query.awaitTermination()
