from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as spark_sum, avg
import os
# Esto asegura que Python pueda encontrar java.exe y winutils.exe


# ==========================================
# CONFIGURACIÓN ENTORNO Y GOOGLE CLOUD
# ==========================================
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"..\datalake-proyecto-c29dd470a592.json"

PROJECT = "datalake-proyecto"
TEMP_BUCKET = "gs://mi-bucket-temporal"  # Debe existir
DATASET = "music_dw"
TABLE_FULL = f"{DATASET}.events_full"
DIM_TABLE = f"{DATASET}.dim_artist"  # Tabla de dimensiones ejemplo
TABLE_TARGET = f"{DATASET}.tabla_analitica_final"

# ==========================================
# INICIALIZACIÓN DE SPARK
# ==========================================
spark = SparkSession.builder \
    .appName("Adhoc_BigQuery_ETL") \
    .config("spark.jars.packages",
            "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.36.1,"
            "com.google.cloud.bigdataoss:gcs-connector:hadoop2-2.2.5") \
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile",
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"]) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("✓ Spark Session iniciada correctamente")

# ==========================================
# CARGA DEL HISTÓRICO COMPLETO DE BIGQUERY
# ==========================================
print("▶ Leyendo events_full desde BigQuery...")
df_events = spark.read.format("bigquery") \
    .option("table", TABLE_FULL) \
    .option("parentProject", PROJECT) \
    .load()

print(f"✓ Eventos cargados: {df_events.count()} filas")

# ==========================================
# CARGA DE TABLA DE DIMENSIONES
# ==========================================
df_dim = spark.read.format("bigquery") \
    .option("table", DIM_TABLE) \
    .option("parentProject", PROJECT) \
    .load()

print(f"✓ Tabla de dimensiones cargada: {df_dim.count()} filas")

# ==========================================
# TRANSFORMACIONES / ANÁLISIS COMPLEJO
# ==========================================
# Calcular KPIs
df_kpis = df_events.groupBy("artist_name").agg(
    count("*").alias("total_events"),
    spark_sum("playcount").alias("total_playcount"),
    avg("playcount").alias("avg_playcount")
)

# Enriquecer con tabla de dimensiones
df_enriched = df_kpis.join(df_dim, on="artist_name", how="left")

# ==========================================
# ESCRITURA DE RESULTADO EN BIGQUERY
# ==========================================
print(f"▶ Escribiendo tabla analítica final en {TABLE_TARGET} (overwrite)...")
df_enriched.write \
    .format("bigquery") \
    .option("table", TABLE_TARGET) \
    .option("parentProject", PROJECT) \
    .option("temporaryGcsBucket", TEMP_BUCKET) \
    .mode("overwrite") \
    .save()

print("✓ Escritura finalizada correctamente")
