from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, expr, window, count, avg, sum as spark_sum
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
import os
# ==========================================
# CONFIG DE RED Y VARIABLES DE ENTORNO
# ==========================================
# FIX para el error de BlockManager en Windows
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
os.environ["SPARK_LOCAL_HOSTNAME"] = "localhost"
 
# JSON de credenciales de Google Cloud
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\walfr\keys\datalake-proyecto-c29dd470a592.json"
 
# ==========================================
# CONFIG DE BIGQUERY
# ==========================================
LIMIT = 20_000_000  # Límite de filas
BUCKET = "bucket-bigdata-music"  # Bucket temporal en GCS
PROJECT = "datalake-proyecto"
DATASET = "music_dw"
TABLE_FULL = f"{DATASET}.events_full"
TABLE_PART = f"{DATASET}.events_partitioned"
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
# SPARK SESSION - SIN GCS-CONNECTOR
# ==========================================
spark = SparkSession.builder \
    .appName("KafkaToBigQuery_Fase2") \
    .master("local[*]") \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
            "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.36.1") \
    .config("spark.driver.memory", "8g") \
    .config("spark.executor.memory", "8g") \
    .config("spark.driver.extraJavaOptions", "-Dio.netty.tryReflectionSetAccessible=true") \
    .config("spark.executor.extraJavaOptions", "-Dio.netty.tryReflectionSetAccessible=true") \
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile",
            r"C:\Users\walfr\keys\datalake-proyecto-c29dd470a592.json") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")
acc = spark.sparkContext.accumulator(0)
print("=" * 60)
print("✓ Spark Session iniciada correctamente")
print(f"✓ Spark UI disponible en: http://localhost:4040")
print("=" * 60)
# ==========================================
# LECTURA DESDE KAFKA
# ==========================================
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "music_stream") \
    .option("startingOffsets", "earliest") \
    .load()
print("✓ Conexión a Kafka establecida")
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
print("✓ Transformaciones de limpieza aplicadas")
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
print("✓ Agregaciones por ventana de tiempo configuradas")
# ==========================================
# FUNCION FOREACHBATCH PARA BIGQUERY
# ==========================================
def process_batch(df, epoch_id):
    global acc
    if df.isEmpty():
        print(f"→ Batch {epoch_id}: vacío, saltando...")
        return
    # Aplanar la estructura de ventana para BigQuery
    df_flat = df.select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        "artist_name",
        "total_eventos",
        "suma_playcount",
        "promedio_playcount"
    )
    batch_count = df_flat.count()
    acc += batch_count
    print(f"→ Batch {epoch_id}: {batch_count} filas | Total acumulado: {acc.value}")
    try:
        # Guardar en tabla completa
        print(f"  ⏳ Escribiendo en {TABLE_FULL}...")
        df_flat.write \
            .format("bigquery") \
            .option("table", TABLE_FULL) \
            .option("temporaryGcsBucket", BUCKET) \
            .option("project", PROJECT) \
            .option("writeMethod", "direct") \
            .mode("append") \
            .save()
        print(f"  ✓ Datos guardados en {TABLE_FULL}")
        # Guardar tabla particionada
        print(f"  ⏳ Escribiendo en {TABLE_PART}...")
        df_flat.write \
            .format("bigquery") \
            .option("table", TABLE_PART) \
            .option("temporaryGcsBucket", BUCKET) \
            .option("project", PROJECT) \
            .option("partitionField", "window_start") \
            .option("partitionType", "DAY") \
            .option("writeMethod", "direct") \
            .mode("append") \
            .save()
        print(f"  ✓ Datos guardados en {TABLE_PART}")
    except Exception as e:
        print(f"  ✗ Error al guardar batch {epoch_id}: {str(e)}")
        # Intentar guardar solo en la tabla principal sin particionamiento
        print(f"  ⚠️  Reintentando solo con tabla completa...")
        try:
            df_flat.write \
                .format("bigquery") \
                .option("table", TABLE_FULL) \
                .option("temporaryGcsBucket", BUCKET) \
                .option("project", PROJECT) \
                .option("writeMethod", "direct") \
                .mode("append") \
                .save()
            print(f"  ✓ Guardado exitoso en {TABLE_FULL}")
        except Exception as e2:
            print(f"  ✗ Error crítico: {str(e2)}")
            raise e2
    # Detener cuando se alcance el límite
    if acc.value >= LIMIT:
        print("=" * 60)
        print("✔ Se alcanzó el límite de 20M. Deteniendo stream…")
        print("=" * 60)
        if len(spark.streams.active) > 0:
            spark.streams.active[0].stop()
# ==========================================
# INICIAR STREAM
# ==========================================
query = df_windowed.writeStream \
    .foreachBatch(process_batch) \
    .option("checkpointLocation", "checkpoint_bigquery") \
    .start()
print("=" * 60)
print("🚀 STREAMING INICIADO")
print("📊 Esperando datos de Kafka (topic: music_stream)...")
print("🎯 Objetivo: 20 millones de registros")
print("=" * 60)
try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("\n⚠️  Streaming detenido por el usuario")
    query.stop()
    print("✓ Proceso finalizado")