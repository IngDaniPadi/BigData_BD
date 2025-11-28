from google.cloud import storage
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
KEYFILE = os.path.abspath(os.path.join(BASE_DIR, "..", "datalake-proyecto-c29dd470a592.json"))


# Ruta al JSON de la cuenta de servicio
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = KEYFILE

bucket_name = "bucket-etl-bigdata"

client = storage.Client()

try:
    bucket = client.get_bucket(bucket_name)
    print(f"✓ Conexión exitosa al bucket: {bucket.name}")
    print(f"Clase de almacenamiento: {bucket.storage_class}")
    print(f"Ubicación: {bucket.location}")
    
    # Listar los primeros 10 objetos
    blobs = list(bucket.list_blobs(max_results=10))
    if blobs:
        print("Objetos en el bucket:")
        for blob in blobs:
            print(f"- {blob.name}")
    else:
        print("El bucket está vacío o no tienes permisos para listar objetos.")
        
except Exception as e:
    print(f"❌ Error al conectar con el bucket: {e}")
