# ============================================================
# PySpark Batch ETL - 3.1
# ============================================================
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os

# -------------------------
# CONFIG: EDITA ESTAS VARIABLES
# -------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
KEYFILE = os.path.join(BASE_DIR, "..", "datalake-proyecto-c29dd470a592.json")
KEYFILE = os.path.abspath(KEYFILE)
PROJECT = "datalake-proyecto"                  # <-- CAMBIA si aplica
DATASET = "music_dw"                           # <-- CAMBIA si aplica
TABLE_FULL = f"{PROJECT}.{DATASET}.events_full"
TABLE_PART = f"{PROJECT}.{DATASET}.events_partitioned"
TABLE_DIM = f"{PROJECT}.{DATASET}.dim_country" # Ajusta si tu tabla dimensional tiene otro nombre
TABLE_TARGET = f"{PROJECT}.{DATASET}.tabla_analitica_final"
BUCKET = "bucket-bigdata-music"                # <-- CAMBIA por tu bucket temporal en GCS

# -------------------------
# Credenciales (entorno)
# -------------------------
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = KEYFILE

# -------------------------
# Spark session
# -------------------------
spark = SparkSession.builder \
    .appName("PySpark_BigQuery_Batch_ETL_3_1") \
    .master("local[*]") \
    .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.36.1") \
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", KEYFILE) \
    .config("spark.sql.broadcastTimeout", "36000") \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("✓ Spark iniciada")

# -------------------------
# Lectura (batch) - Opción: unionar o elegir tabla particionada
# -------------------------
# Recomendación: si quieres procesar TODO el histórico -> usa events_full.
# Si quieres probar / iterar más rápido -> leer solo events_partitioned.
#
# Aquí te doy 2 opciones; la variable READ_MODE controla:
# - 'full' -> lee events_full
# - 'partitioned' -> lee events_partitioned
# - 'union' -> union all (events_full + events_partitioned)  (usar con cuidado para duplicados)
READ_MODE = "full"  # "full" | "partitioned" | "union"

if READ_MODE == "full":
    print("▶ Leyendo events_full (histórico completo)")
    df_events = spark.read.format("bigquery").option("table", TABLE_FULL).load()
elif READ_MODE == "partitioned":
    print("▶ Leyendo events_partitioned (particiones)")
    df_events = spark.read.format("bigquery").option("table", TABLE_PART).load()
else:
    print("▶ Leyendo y unionando ambas tablas (union all) - cuidado duplicados")
    df1 = spark.read.format("bigquery").option("table", TABLE_FULL).load()
    df2 = spark.read.format("bigquery").option("table", TABLE_PART).load()
    df_events = df1.unionByName(df2)

print(f"✓ Eventos cargados: {df_events.count():,}")

# -------------------------
# Lectura tabla dimensional
# -------------------------
print("▶ Leyendo tabla dimensional (dim_country)")
df_dim = spark.read.format("bigquery").option("table", TABLE_DIM).load()
print(f"✓ Dimensión cargada: {df_dim.count():,}")

# -------------------------
# Enriquecimiento: join por 'country' (ajusta campo si tu dim usa otro)
# -------------------------
print("▶ Realizando JOIN (left) con dimensión por 'country' ...")
if "country" in df_events.columns and "country" in df_dim.columns:
    df_enriched = df_events.join(df_dim, on="country", how="left")
else:
    # Si no existe 'country', solo añade df_dim como cross? -> aquí asumimos coincidencia en 'country'
    print("⚠ Atención: no se encontró columna 'country' en alguno de los datasets. Ajusta la clave de join.")
    df_enriched = df_events

print("✓ Enriquecimiento realizado")

# -------------------------
# Transformaciones adicionales y KPIs (ejemplos)
# -------------------------
print("▶ Calculando KPIs y columnas derivadas...")

# Ejemplo: crear columna date y hour a partir de window_start
df_enriched = df_enriched.withColumn("window_date", F.to_date(F.col("window_start")))

# KPIs agregados por artista y grupo de país (country_group viene de dim_country; si no, usa country)
group_col = "country_group" if "country_group" in df_enriched.columns else "country"

df_kpis = df_enriched.groupBy("artist_name", group_col).agg(
    F.count("*").alias("total_eventos"),
    F.sum(F.col("suma_playcount")).alias("total_playcount"),     # suma de suma_playcount si ya venía pre-agg
    F.avg(F.col("promedio_playcount")).alias("avg_promedio_playcount"),
    F.sum(F.col("total_eventos")).alias("sum_total_eventos"),    # si tu tabla ya trae total_eventos ventana
    F.approx_count_distinct("user_id").alias("usuarios_unicos"),
    F.max("window_end").alias("ultimo_evento"),
    F.min("window_start").alias("primer_evento")
)

# Calcular ratios y métricas derivadas
df_final = df_kpis.withColumn(
    "playcount_por_usuario",
    F.when(F.col("usuarios_unicos") > 0, F.col("total_playcount") / F.col("usuarios_unicos")).otherwise(None)
).withColumn(
    "periodo_days",
    (F.datediff(F.col("ultimo_evento").cast("date"), F.col("primer_evento").cast("date")))
)

print("✓ KPIs calculados")

# -------------------------
# Guardar tabla analítica final en BigQuery
# -------------------------
# Recomendación: particionar la tabla por window_date o window_start (DAY) para consultas baratas.
print(f"▶ Escribiendo tabla analítica final en {TABLE_TARGET} (overwrite) ...")

(df_final
 .write
 .format("bigquery")
 .option("table", TABLE_TARGET)
 .option("temporaryGcsBucket", BUCKET)
 .option("partitionField", "primer_evento")   # puedes cambiar por 'ultimo_evento' o 'window_date'
 .option("partitionType", "DAY")
 .mode("overwrite")
 .save()
)

print("✓ Tabla analítica final escrita en BigQuery")
print("FIN")
