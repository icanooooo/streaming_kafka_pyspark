from confluent_kafka import SerializingProducer
import json
import time

producer = SerializingProducer({"bootstrap.servers":"localhost:9092"})

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