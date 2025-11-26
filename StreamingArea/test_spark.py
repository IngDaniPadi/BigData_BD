from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr

spark = SparkSession.builder \
    .appName("KafkaTest") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .config("spark.executor.memory", "2g") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# === 1. LEER KAFKA ===
df_raw = spark.readStream \
   .format("kafka") \
   .option("kafka.bootstrap.servers", "localhost:9092") \
   .option("subscribe", "music_stream") \
   .option("startingOffsets", "latest") \
   .load()

# === 2. TRANSFORMACIONES ===
# convertir value de bytes → string
df_clean = df_raw.select(
    col("timestamp"),
    col("partition"),
    col("offset"),
    expr("CAST(value AS STRING)").alias("value")
)

# === 3. ESCRITURA EN CONSOLA ===
query = df_clean.writeStream \
   .format("console") \
   .option("truncate", False) \
   .option("numRows", 20) \
   .start()

query.awaitTermination()
