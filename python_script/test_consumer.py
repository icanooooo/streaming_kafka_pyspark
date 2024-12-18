import simplejson as json

from helper.postgres_helper import create_connection, load_query, print_query
from confluent_kafka import Consumer, KafkaError, KafkaException

# conf = {
#     'bootstrap.servers':'localhost:9092' #, localhost:9093, localhost:9094"
# }

# # Pelajari lagi configuration consumer detail seperti apa
# consumer = Consumer(conf | {
#     'group.id':'test-group', 
#     'auto.offset.reset':'earliest',
#     'enable.auto.commit': False 
# })

if __name__ == "__main__":
    # consumer.subscribe(['test_event'])

    # try:
    #     while True:
    #         msg = consumer.poll(timeout=1.0) # timeout waktu delay jika message yang didapatkan none
    #         if msg is None:
    #             continue
    #         elif msg.error():
    #             if msg.error().code() == KafkaError.__PARTITION_EOF:
    #                 continue
    #             else:
    #                 print(msg.error())
    #                 break
    #         else:
    #             received = json.loads(msg.value().decode('utf-8'))
    #             print(received)
    # except KafkaException as e:
    #     print(e)

    conn = create_connection('localhost', 5432, 'destination_db', 'icanooo', 'rahasia')

    query = """
    CREATE TABLE IF NOT EXISTS test (
        Nama VARCHAR(50),
        umur INT
    );
"""

    load_query(conn, query)

    query2 = "INSERT INTO test (Nama, umur) VALUES('ican', 26);"

    load_query(conn, query2)

    result = print_query(conn, "SELECT * FROM test;")
    print(result)

    conn.commit() #ini jangan lupa penting banget
    conn.close()