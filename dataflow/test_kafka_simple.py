"""
Script simple para verificar que podemos leer de Kafka
"""
from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'spotify-events',
    bootstrap_servers=['localhost:29092'],
    auto_offset_reset='latest',
    enable_auto_commit=True,
    group_id='test-consumer',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Escuchando eventos de Kafka...")
print("Presiona Ctrl+C para detener\n")

try:
    for message in consumer:
        event = message.value
        print(f"Evento recibido: {event['event_type']} - Track: {event['track_id']} - User: {event['user_id']}")
except KeyboardInterrupt:
    print("\n\nDetenido por usuario")
finally:
    consumer.close()
