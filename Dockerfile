FROM python:3.10-slim

WORKDIR /app

# Consideration to use to automatically run pyspark_consumer program

COPY python_script/pyspark_consumer.py python_script/streaming_simulation.py run_program.sh /app/

RUN pip install pyspark \
    psycopg2-binary\
    kafka-python