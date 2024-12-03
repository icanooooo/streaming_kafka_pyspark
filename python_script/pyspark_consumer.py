from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructType, StructField, TimestampType, IntegerType, FloatType
from pyspark.sql.functions import from_json, col, udf
from helper.postgres_helper import create_connection, load_query
# from forex_python.converter import CurrencyRates

def ensureTable():
    conn = create_connection('localhost', 5432, 'destination_db', 'icanooo', 'rahasia')

    query = """
    CREATE TABLE IF NOT EXISTS test_table (
        id VARCHAR(50),
        name VARCHAR(50),
        job varchar(50),
        age INT,
        salary_in_usd FLOAT,
        submitted_time TIMESTAMP 
    );
    """

    load_query(conn, query)

    conn.commit()
    conn.close()

def to_postgres(batch_df, batch_id):
    try:
        batch_df.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://localhost:5432/destination_db") \
            .option("dbtable", "test_table") \
            .option("user", "icanooo") \
            .option("password", "rahasia") \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()
    except Exception as e:
        print(f"error: {e}")

if __name__ == "__main__":
    ensureTable()

    # c = CurrencyRates()
    idr_to_usd = 0.000063 # don't forget to use forex_python
    
    # Create SparkSession
    spark = SparkSession.builder \
        .appName("Test-PySpark") \
        .master("local[*]") \
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .config("spark.jars",
                "/home/icanooo/Desktop/DE/streaming_project/postgresql-42.7.4.jar") \
        .getOrCreate()

    # kafka configuration
    kafka_broker = "localhost:9092"
    kafka_topic = "test_event"

    # read stream
    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_broker) \
        .option("subscribe", kafka_topic) \
        .option("startingOffsets", "earliest") \
        .load()

    # Defining Schema
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("job", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("salary", IntegerType(), True),
        StructField("submitted_time", TimestampType(), True)
    ])

    parsed_stream = raw_stream.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")

    parsed_stream = parsed_stream.withColumn("salary_in_usd", col('salary') * idr_to_usd) \
                    .drop('salary')

    # Display data
    query = parsed_stream.writeStream \
        .foreachBatch(to_postgres) \
        .outputMode("append") \
        .start()

    query.awaitTermination()