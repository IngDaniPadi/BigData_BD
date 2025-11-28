from pyspark.sql import SparkSession

# Ruta al JSON de tu service account
KEYFILE = r"E:\Usuarios\Personal\Desktop\Daniel\gcs-key.json"

# Ruta de prueba en GCS (tu bucket)
GCS_TEST_PATH = "gs://tu-bucket-prueba/test_folder/"

# Iniciar Spark
spark = (
    SparkSession.builder
    .appName("Spark-GCS-Test")
    .master("local[*]")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config(
        "spark.jars.packages",
        "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.36.1,"
        "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.14-shaded"
    )
    .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
    .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", KEYFILE)
    .getOrCreate()
)

print("✓ Spark iniciado correctamente")

# Crear DataFrame de prueba
data = [("Daniel", 28), ("Ana", 30)]
df = spark.createDataFrame(data, ["Nombre", "Edad"])

# Intentar escribir a GCS
try:
    df.write.mode("overwrite").parquet(GCS_TEST_PATH)
    print("✓ Escritura en GCS exitosa")
except Exception as e:
    print("✗ Error escribiendo en GCS:", e)
finally:
    spark.stop()
