from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, expr, window, count, avg, sum as spark_sum
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
import os

# ==========================================
# CONFIG
# ==========================================
LIMIT = 20_000_000  # Límite de filas
BUCKET = "bucket-bigdata-music"  # Bucket temporal en GCS
PROJECT = "datalake-proyecto"
DATASET = "music_dw"

TABLE_FULL = f"{DATASET}.events_full"
TABLE_PART = f"{DATASET}.events_partitioned"

# JSON de credenciales de Google Cloud
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\walfr\keys\datalake-proyecto-c29dd470a592.json"

# ==========================================
# ESQUEMA DEL JSON DE KAFKA
# ==========================================
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

# ==========================================
# SPARK SESSION
# ==========================================
spark = SparkSession.builder \
    .appName("KafkaToBigQuery_Fase2") \
    .config("spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
        "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.36.1,"
        "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.13") \
    .config("spark.driver.memory", "8g") \
    .config("spark.executor.memory", "8g") \
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile",
            r"C:\Users\walfr\keys\datalake-proyecto-c29dd470a592.json") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
acc = spark.sparkContext.accumulator(0)

# ==========================================
# LECTURA DESDE KAFKA
# ==========================================
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "music_stream") \
    .option("startingOffsets", "earliest") \
    .load()

# ==========================================
# PARSEAR JSON + LIMPIEZA
# ==========================================
df_json = df_raw.select(
    from_json(expr("CAST(value AS STRING)"), schema).alias("data")
).select("data.*")

df_clean = df_json \
    .dropna(subset=["timestamp", "artist_name"]) \
    .filter(col("playcount").isNotNull()) \
    .withColumn("event_date", expr("DATE(timestamp)"))

# ==========================================
# PRE-AGREGACIÓN (VENTANA 5 MIN)
# ==========================================
df_windowed = df_clean \
    .withWatermark("timestamp", "5 minutes") \
    .groupBy(
        window("timestamp", "5 minutes"),
        "artist_name"
    ).agg(
        count("*").alias("total_eventos"),
        spark_sum("playcount").alias("suma_playcount"),
        avg("playcount").alias("promedio_playcount")
    )

# ==========================================
# FUNCION FOREACHBATCH PARA BIGQUERY
# ==========================================
def process_batch(df, epoch_id):
    global acc

    batch_count = df.count()
    acc += batch_count
    print(f"→ Batch {epoch_id}: {batch_count} filas | Total acumulado: {acc.value}")

    # Guardar en tabla completa
    df.write \
        .format("bigquery") \
        .option("table", TABLE_FULL) \
        .option("temporaryGcsBucket", BUCKET) \
        .option("project", PROJECT) \
        .mode("append") \
        .save()

    # Guardar tabla particionada
    df.write \
        .format("bigquery") \
        .option("table", TABLE_PART) \
        .option("temporaryGcsBucket", BUCKET) \
        .option("project", PROJECT) \
        .mode("append") \
        .save()

    # Detener cuando se alcance el límite
    if acc.value >= LIMIT:
        print("✔ Se alcanzó el límite de 20M. Deteniendo stream…")
        spark.streams.active[0].stop()

# ==========================================
# INICIAR STREAM
# ==========================================
query = df_windowed.writeStream \
    .foreachBatch(process_batch) \
    .option("checkpointLocation", "checkpoint_bigquery") \
    .start()

query.awaitTermination()
