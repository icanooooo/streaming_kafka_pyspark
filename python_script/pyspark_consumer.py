from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructType, StructField, TimestampType
from pyspark.sql.functions import from_json, col

if __name__ == "__main__":
    # Create SparkSession
    spark = SparkSession.builder \
        .appName("Test-PySpark") \
        .master("local[*]") \
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .config("spark.jars.repositories", "https://repo1.maven.org/maven2") \
        .getOrCreate()

    # kafka configuration
    kafka_broker = "localhost:9092"
    kafka_topic = "test_event"

    # read stream
    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_broker) \
        .option("subscribe", kafka_topic) \
        .load()

    # Defining Schema
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("age", StringType(), True),
        StructField("submitted_time", TimestampType(), True)
    ])

    parsed_stream = raw_stream.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")

    # Display data
    query = parsed_stream.writeStream \
        .outputMode("append") \
        .format("console") \
        .start()

    query.awaitTermination