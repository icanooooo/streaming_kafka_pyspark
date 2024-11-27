#!/bin/bash
bin/kafka-topics.sh --create --topic  test_data --bootstrap-server localhost:9092

# python python_script/streaming_simulation.py
# python python_script/pyspark_consumer.py
