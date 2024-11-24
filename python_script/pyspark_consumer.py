from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType, TimestampType, StructField
from pyspark.sql.functions import from_json, col
import psycopg2

schema = StructType([
    StructField("id", IntegerType(), True),
    StructType("name", StringType(), True),
    StructType("age", IntegerType(), True),
    StructType("timestamp", TimestampType(), True)
])

spark = SparkSession.builder.appName("Testing Postgres").getOrCreate()

df = spark.readStream.format("kafka")\
    .option("kafka.bootstrap.servers", "kafka:9092")\
    .option("subscribe", "test_data")\
    .load()

parsed = df.select(from_json(col("value").cast("string"), schema).alias("data"))

def write_to_postgres(batch_df, batch_id):
    batch_df.write\
        .format("jdbc")\
        .option("url", "jdbc:postgresql://postgres:5432/destination_db")\
        .option("test_table", "test_data")\
        .option("user", "icanooo")\
        .option("password", "rahasia")\
        .mode("append")\
        .save()

