from kafka import KafkaProducer
import time

producer = KafkaProducer(bootstrap_servers='localhost:9092')

for i in range(10):
    msg = f"mensaje {i}"
    producer.send("music_stream", msg.encode("utf-8"))
    print("Enviado:", msg)
    time.sleep(1)

producer.flush()
