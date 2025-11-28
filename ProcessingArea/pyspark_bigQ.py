from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as sum_, avg
import os

# -------------------------------
# CONFIGURACIONES
# -------------------------------
PROJECT = "datalake-proyecto"  # Tu ID de proyecto en GCP
TEMP_BUCKET = "gs://mi-bucket-temporal"  # Bucket temporal de GCS (debe existir)
TABLE_FULL = "music_dw.events_full"  # Tabla de BigQuery de origen
TABLE_TARGET = "music_dw.tabla_analitica_final"  # Tabla destino en BigQuery
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
KEYFILE = os.path.abspath(os.path.join(BASE_DIR, "..", "datalake-proyecto-c29dd470a592.json"))
# Ruta a la credencial JSON de servicio
CRED_JSON = KEYFILE

# -------------------------------
# INICIALIZACIÓN DE SPARK
# -------------------------------
spark = SparkSession.builder \
    .appName("PySpark-BigQuery") \
    .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.36.1") \
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", CRED_JSON) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("✓ Spark iniciada correctamente")

# -------------------------------
# LECTURA DE BIGQUERY
# -------------------------------
print("▶ Leyendo events_full (histórico completo)")
df_events = spark.read.format("bigquery") \
    .option("table", TABLE_FULL) \
    .option("parentProject", PROJECT) \
    .load()

print(f"✓ Eventos cargados: {df_events.count()}")

# -------------------------------
# PROCESAMIENTO (KPIs)
# -------------------------------
df_kpis = df_events.groupBy("artist_name").agg(
    count("total_eventos").alias("total_events"),
    sum_("suma_playcount").alias("total_playcount"),
    avg("promedio_playcount").alias("avg_playcount")
)

print("✓ KPIs calculados correctamente")

# -------------------------------
# ESCRITURA EN BIGQUERY
# -------------------------------
print(f"▶ Escribiendo tabla analítica final en {TABLE_TARGET} (overwrite)...")
(df_kpis.write
 .format("bigquery")
 .option("table", TABLE_TARGET)
 .option("parentProject", PROJECT)  # Obligatorio para evitar error de project ID
 .option("temporaryGcsBucket", TEMP_BUCKET)  # Obligatorio para escritura indirecta
 .mode("overwrite")
 .save()
)

print("✓ Escritura finalizada correctamente")
