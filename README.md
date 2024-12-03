# Data Streaming with Apache Kafka & PySpark

This repository are for learning purposes for creating Data Streaming pipelines with Apache Kafka & Spark (PySpark). The project idea is simple, create a pipeline which use a python program to takes input as the kafka producer and then consumed by Apache Spark to process (changing 'salary' column to usd) and store it in local Postgres (or BigQuery in the future) within docker. Please see project diagram below:

<img src='assets/Project_Diagram_Local.png'  alt='project diagram' width='75%'>

### Apache Kafka

So Apache Kafka is a distribute event and stream processing tool. There is a producer which logs an event and a consumer who consume the logs of the event. Apache Kafka uses topic that the consumer subscribes to, ensuring the consumer only get the Data they need. Imagine kafka is the middleman between two services making sure communication between the two service are correct.

Within Kafka there are brokers, nodes that are responsible for doing the work. Kafka is a distributed system, therefore we can easily add brokers to adjust neccessity of the data stream. They are also fault tolerance, with brokers backing up each other, ensuring if any of the broker are shut down another one could take the load. A group of multiple brokers are called clusters.

### Apache Spark (PySpark)

Spark is a distributed system data processing tool. It aims to create a scalable data process by it's distributed system design and in-memory processing. To use apache spark with python, we use its API, PySpark.

### To Do

By using Apache Kafka & Spark, we created a streaming pipeline that listens to an event and logs it to a consumer. This project is fairly simple, aiming to show how to use these services within docker. First all, in the directory where we pull this project, in the terminal input below to run all of the docker services in the docker-compose file

```
docker compose up
```

I've already build a service that automaticly run `kafka-topics` command to create kafka topic. With this, please go the the `python_script` directory and run the pyspark consumer python program.

```
python3 pyspark_consumer.py 
```

After that, if you want to input a log to the kafka broker, run the streaming simulation python file.

```
python3 streaming_simulation.py
```

### Summary

In summary, this project is just a simple streaming pipeline which takes input, a processing layer with pyspark, and to store it in a database. I know this may seem pointless, as we can just take input an directly store it in a database. But I think this project could be maybe a template or example for further projects as it is quite simple an easy to understand, again this project is just for me to learn how to use kafka in conjuction with spark.

After this I want to create project using kafka and pyspark with this as reference, I will update this as soon as the project comes up. 

Shoutout to [CodeWithYu](https://www.youtube.com/@CodeWithYu) as his [project](https://github.com/airscholar/realtime-voting-data-engineering) was a direct inspiration and a learning resource for my project.

Any comments are welcome. Thank you!
