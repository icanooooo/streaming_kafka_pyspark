import simplejson as json

from confluent_kafka import Consumer, KafkaError, KafkaException

conf = {
    'bootstrap.servers':'localhost:9092'
}

# Pelajari lagi configuration consumer detail seperti apa
consumer = Consumer(conf | {
    'group.id':'test-group', 
    'auto.offset.reset':'earliest',
    'enable.auto.commit': False 
})

if __name__ == "__main__":
    consumer.subscribe(['test_event'])

    try:
        while True:
            msg = consumer.poll(timeout=1.0) # timeout waktu delay jika message yang didapatkan none
            if msg is None:
                continue
            elif msg.error():
                if msg.error().code() == KafkaError.__PARTITION_EOF:
                    continue
                else:
                    print(msg.error())
                    break
            else:
                received = json.loads(msg.value().decode('utf-8'))
                print(received)
    except KafkaException as e:
        print(e)