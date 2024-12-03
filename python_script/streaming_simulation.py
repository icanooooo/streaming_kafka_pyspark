from confluent_kafka import Producer #find out kenapa pakai serializing producer bukan producer aja
import json
import time
from datetime import datetime

producer = Producer({"bootstrap.servers":"localhost:9092"}) #, localhost:9093, localhost:9094"})

def get_input():
    data = {}

    data['id'] = input("Please insert id: ")
    data['name'] = input("Please insert name: ")
    data['job'] = input("Please insert job: ")
    
    while True:
        age = input("Please insert AGE: ")

        try:
            data['age'] = int(age)
            break
        except:
            print("Please input a number")
            continue
    
    while True:
        salary = input("Please insert salary in Rp: ")

        try:
            data['salary'] = int(salary)
            break
        except:
            print("Please input a number")
            continue

    data['submitted_time'] = time.time()

    return data
    
def delivery_report(err, msg):
    if err is not None:
        print(f"message delivery failed: {err}")
    else:
        print(f"message delivered to {msg.topic()} [{msg.partition()}]")

def send_messages():
    data = get_input()

    producer.produce(
        "test_event",
        key=data['id'],
        value=json.dumps(data),
        on_delivery=delivery_report
    )

    producer.flush()

send_messages()