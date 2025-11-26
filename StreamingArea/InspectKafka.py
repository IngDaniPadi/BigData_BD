from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("InspectKafka") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

df = spark.readStream \
   .format("kafka") \
   .option("kafka.bootstrap.servers", "localhost:9092") \
   .option("subscribe", "music_stream") \
   .option("startingOffsets", "latest") \
   .load() \
   .selectExpr("CAST(value AS STRING) AS json")

query = df.writeStream \
   .format("console") \
   .option("truncate", False) \
   .start()

query.awaitTermination()
