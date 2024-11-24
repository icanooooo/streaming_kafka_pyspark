from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def send_messages():
    data = {
        "id": 28,
        "name": "ican",
        "age": 24,
        "submitted_time": time.time()
    }

    producer.send("test_data", value=data)

send_messages()