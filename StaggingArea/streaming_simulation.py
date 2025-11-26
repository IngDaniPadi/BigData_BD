import duckdb
import pandas as pd
import json
import uuid
import time
from kafka import KafkaProducer

# ================================
# CONFIGURACIÓN
# ================================

DUCKDB_PATH = "music.duckdb"
TOPIC = "music_stream"
BOOTSTRAP = "localhost:9092"
CHUNK_SIZE = 100_000      # 100k por lote
TARGET_EVENTS = 50_000_000

# ================================
# CONEXIÓN A DUCKDB
# ================================

print("Conectando a DuckDB...")
con = duckdb.connect(DUCKDB_PATH)

# ================================
# CARGAR TABLA TRACKS
# ================================

print("Cargando user_top_tracks...")
tracks_query = """
SELECT 
    u.user_id,
    u.country,
    t.track_name AS name,
    t.artist_name,
    t.playcount,
    t.rank,
    'track' AS type
FROM user_top_tracks t
JOIN users u ON u.user_id = t.user_id
"""
df_tracks = con.execute(tracks_query).fetchdf()
print(f"✔ Filas tracks: {len(df_tracks):,}")

# ================================
# CARGAR TABLA ARTISTS
# ================================

print("Cargando user_top_artists...")
artists_query = """
SELECT 
    u.user_id,
    u.country,
    a.artist_name AS name,
    a.artist_name,
    a.playcount,
    a.rank,
    'artist' AS type
FROM user_top_artists a
JOIN users u ON u.user_id = a.user_id
"""
df_artists = con.execute(artists_query).fetchdf()
print(f"✔ Filas artists: {len(df_artists):,}")

# ================================
# CALCULAR SAMPLE EXTRA PARA 50M
# ================================

total_base = len(df_tracks) + len(df_artists)
missing = TARGET_EVENTS - total_base

print("\nRESUMEN BASE")
print(f"Total base: {total_base:,}")
print(f"Faltan para llegar a 50M: {missing:,}")

if missing > 0:
    print(f"Generando sample de {missing:,} filas extra...")
    df_sample = df_tracks.sample(missing, replace=True)
else:
    df_sample = pd.DataFrame()
    print("✔ No se necesita sample extra.")

# ================================
# UNIÓN FINAL
# ================================

print("\nConstruyendo dataset final (50M)...")
df_final = pd.concat([df_tracks, df_artists, df_sample], ignore_index=True)
print(f"🔥 Total final: {len(df_final):,}")

# ================================
# CONFIGURAR PRODUCTOR KAFKA
# ================================

print("\nIniciando productor Kafka...")
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    linger_ms=5,
    batch_size=64 * 1024
)

# ================================
# STREAMING A KAFKA
# ================================

print("\nIniciando transmisión a Kafka...\n")

offset = 0
total_rows = len(df_final)

while offset < total_rows:
    chunk = df_final.iloc[offset : offset + CHUNK_SIZE]

    print(f"Enviando lote desde offset {offset:,} ...")

    for _, row in chunk.iterrows():
        event = {
            "event_id": str(uuid.uuid4()),
            "user_id": row["user_id"],
            "country": row["country"],
            "name": row["name"],
            "artist_name": row["artist_name"],
            "playcount": int(row["playcount"]),
            "rank": int(row["rank"]),
            "type": row["type"],
            "timestamp": pd.Timestamp.now().isoformat()
        }

        producer.send(TOPIC, event)

    producer.flush()
    offset += CHUNK_SIZE
    time.sleep(0.02)

print("\n✔ Streaming finalizado correctamente.")
