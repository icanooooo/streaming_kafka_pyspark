FROM python:3.10-slim

WORKDIR /app

COPY python_script/pyspark_consumer.py python_script/streaming_simulation.py run_program.sh /app/

RUN pip install pyspark \
    psycopg2-binary

RUN chmod +x run_program.sh

CMD ["./run_program.sh"]