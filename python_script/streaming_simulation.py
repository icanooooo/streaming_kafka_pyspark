from confluent_kafka import Producer #find out kenapa pakai serializing producer bukan producer aja
import json
import time
from datetime import datetime

producer = Producer({"bootstrap.servers":"localhost:9092"}) #, localhost:9093, localhost:9094"})

def delivery_report(err, msg):
    if err is not None:
        print(f"message delivery failed: {err}")
    else:
        print(f"message delivered to {msg.topic()} [{msg.partition()}]")

def send_messages():
    data = {
        "id": input("Please insert id: "),
        "name": input("Please insert name: "),
        "age": input("Please insert age: "),
        "submitted_time": time.time()
    }

    producer.produce(
        "test_event",
        key=data['id'],
        value=json.dumps(data),
        on_delivery=delivery_report
    )

    producer.flush()

send_messages()