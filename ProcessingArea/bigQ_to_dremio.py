from pyspark.sql import SparkSession
import os

# ==========================================
# CONFIGURACIÓN
# ==========================================
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
os.environ["SPARK_LOCAL_HOSTNAME"] = "localhost"

# Ruta a tu archivo de credenciales JSON
KEYFILE = r"..\datalake-proyecto-c29dd470a592.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = KEYFILE

# Proyectos y dataset
PROJECT = "datalake-proyecto"
DATASET = "music_dw"
TABLE_ANALYTICA = f"{DATASET}.tabla_analitica_final"

# Ruta de exportación a GCS
GCS_OUTPUT_PATH = "gs://mi-bucket-parquet/tabla_analitica_final_parquet"

# ==========================================
# CREAR SPARK SESSION CON CONECTORES
# ==========================================
spark = (
    SparkSession.builder
    .appName("BigQuery-to-GCS-Parquet")
    .master("local[*]")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config(
        "spark.jars.packages",
        "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.36.1,"
        "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.10"
    )
    .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
    .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", KEYFILE)
    .getOrCreate()
)


spark.sparkContext.setLogLevel("WARN")
print("✓ Spark iniciada correctamente")

# ==========================================
# LECTURA DE BIGQUERY
# ==========================================
df_kpis = (
    spark.read.format("bigquery")
    .option("table", TABLE_ANALYTICA)
    .option("parentProject", PROJECT)
    .load()
)
print(f"✓ Leídos {df_kpis.count()} registros desde BigQuery")

# ==========================================
# ESCRITURA A GCS COMO PARQUET
# ==========================================
df_kpis.write \
    .format("parquet") \
    .mode("overwrite") \
    .save(GCS_OUTPUT_PATH)

print(f"✓ DataFrame exportado correctamente a GCS en: {GCS_OUTPUT_PATH}")
