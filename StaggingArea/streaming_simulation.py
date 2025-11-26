""" import duckdb

conDuck = duckdb.connect("music.duckdb")

tables_name = [t[0] for t in conDuck.execute("SHOW TABLES").fetchall()] 

for name in tables_name:
    print(f"Estamos usando la tabla {name}") """

import duckdb
import json
from kafka import KafkaProducer
import pandas as pd
import time

# ------------ CONFIG -----------------

DUCKDB_PATH = "music.duckdb"
TABLE_NAME = "users"            # <-- cámbiala o automatízala
CHUNK_SIZE = 100000              # filas por lote
TOPIC = "music_stream"
BOOTSTRAP_SERVERS = "localhost:9092"

# -------------------------------------

# Conexión a DuckDB
con = duckdb.connect(DUCKDB_PATH)
print("Conexión a duckDB establecida")

# Productor Kafka
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# Obtener número total de filas
total_rows = con.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}").fetchone()[0]
print(f"Total filas: {total_rows}")

offset = 0
batch_number = 1

while offset < total_rows:
    # Leer lote
    df = con.execute(
        f"SELECT * FROM {TABLE_NAME} LIMIT {CHUNK_SIZE} OFFSET {offset}"
    ).fetchdf()

    # Convertir a registros JSON
    records = df.to_dict(orient="records")

    print(f"Enviando lote {batch_number} ({len(records)} filas)")

    # Enviar cada registro a Kafka
    for record in records:
        producer.send(TOPIC, value=record)

    producer.flush()

    offset += CHUNK_SIZE
    batch_number += 1
    time.sleep(0.2)   # opcional: limitar ritmo

print("Streaming finalizado")
