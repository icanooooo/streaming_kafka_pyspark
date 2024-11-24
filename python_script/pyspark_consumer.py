from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, StructField, IntegerType, TimestampType, StructField
from pyspark.sql.functions import from_json, col
import psycopg2

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("timestamp", TimestampType(), True)
])

spark = SparkSession.builder.appName("Testing Postgres").getOrCreate()

df = spark.readStream.format("kafka")\
    .option("kafka.bootstrap.servers", "kafka:9092")\
    .option("subscribe", "test_data")\
    .load()

parsed = df.select(from_json(col("value").cast("string"), schema).alias("data"))

def ensure_table_exist():
    db_config = {
        "dbname": "destination_db",
        "user": "icanooo",
        "password": "rahasia",
        "host": "localhost",
        "port": 5432
    }

    query = """
    CREATE TABLE IF NOT EXIST test_table (
        id SERIAL PRIMARY KEY,
        name STRING,
        age INT,
        submitted_time TIMESTAMPE  
    );"""

    try:
        connection = psycopg2.connect(db_config)
        cursor = connection.cursor()

        cursor.execute(query)

        connection.commit()
    except Exception as e:
        print(f"Error: {e}")

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

def write_to_postgres(batch_df, batch_id):
    ensure_table_exist()
    
    batch_df.write\
        .format("jdbc")\
        .option("url", "jdbc:postgresql://postgres:5432/destination_db")\
        .option("test_table", "test_data")\
        .option("user", "icanooo")\
        .option("password", "rahasia")\
        .mode("append")\
        .save()


parsed.writeStream.foreachBatch(write_to_postgres).start().awaitTermination()