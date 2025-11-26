from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

# =======================
# 1. SPARK SESSION
# =======================
spark = SparkSession.builder \
    .appName("MusicStreamProcessing") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .config("spark.sql.shuffle.partitions", "5") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# =======================
# 2. LEER DATA DE KAFKA
# =======================
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "music_stream") \
    .option("startingOffsets", "latest") \
    .load()

# =======================
# 3. DEFINIR ESQUEMA JSON
# =======================
schema = StructType([
    StructField("event_id", StringType()),
    StructField("user_id", IntegerType()),
    StructField("country", StringType()),
    StructField("name", StringType()),
    StructField("artist_name", StringType()),
    StructField("playcount", IntegerType()),
    StructField("rank", IntegerType()),
    StructField("type", StringType()),
    StructField("timestamp", StringType())
])

# =======================
# 4. PARSEAR JSON
# =======================
json_df = raw_df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# =======================
# 5. LIMPIEZA
# =======================
clean_df = json_df.filter(
    col("user_id").isNotNull() &
    col("artist_name").isNotNull() &
    col("playcount").isNotNull()
)

# Convertir timestamp a tipo fecha
clean_df = clean_df.withColumn(
    "event_time", to_timestamp(col("timestamp"))
)

# =======================
# 6. VENTANA DE 5 MINUTOS
# =======================
windowed_df = clean_df \
    .withWatermark("event_time", "5 minutes") \
    .groupBy(
        window(col("event_time"), "5 minutes"),
        col("artist_name")
    ) \
    .agg(
        count("*").alias("event_count"),
        sum("playcount").alias("total_playcount"),
        avg("playcount").alias("avg_playcount")
    )

# =======================
# 7. OUTPUT EN CONSOLA (para pruebas)
# =======================
query = windowed_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()
