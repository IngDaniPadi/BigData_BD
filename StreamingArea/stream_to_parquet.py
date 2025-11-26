from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, expr,
    window, count, avg, sum as spark_sum
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

# ==========================================
# CONFIG
# ==========================================
LIMIT = 40_000_000
OUTPUT_PATH = "output_parquet"           # Para cargarlo a BigQuery después
CHECKPOINT_PATH = "checkpoint_parquet"   # Control del stream

# Esquema del JSON enviado a Kafka
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
    .appName("KafkaToPySpark_Fase2") \
    .config("spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
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
# PARSEAR JSON + LIMPIEZA BÁSICA
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
# PROCESAMIENTO POR LOTES
# ==========================================
def process_batch(df, epoch_id):
    global acc

    batch_count = df.count()
    acc += batch_count

    print(f"→ Batch {epoch_id}: {batch_count} filas | Total: {acc.value}")

    # Guardar resultado del batch (modo taller)
    df.write.mode("append").parquet(OUTPUT_PATH)

    # Detener cuando llegue a 40M
    if acc.value >= LIMIT:
        print("✔ Se alcanzó el límite de 40M. Deteniendo stream…")
        spark.streams.active[0].stop()

    # ================================
    # MODO BIGQUERY (ACTÍVALO DESPUÉS)
    # ================================
    # df.write \
    #   .format("bigquery") \
    #   .option("table", "proyecto.dataset.tabla") \
    #   .option("temporaryGcsBucket", "mi-bucket-temporal") \
    #   .mode("append") \
    #   .save()

# ==========================================
# INICIAR STREAM
# ==========================================
query = df_windowed.writeStream \
    .foreachBatch(process_batch) \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .start()

query.awaitTermination()
