import os
from kafka import KafkaConsumer
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType




# Set up Spark session
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,org.mongodb.spark:mongo-spark-connector_2.12:10.2.1 pyspark-shell'

spark = SparkSession.builder \
    .appName("KafkaToMongoDB") \
    .getOrCreate()

# .config("spark.mongodb.input.uri", "mongodb://localhost:27017/") \
# .config("spark.mongodb.output.uri", "mongodb://localhost:27017/") \

    # Define the schema for the incoming Kafka messages
schema = StructType([
    StructField("City", StringType(), True),
    StructField("Datetime", StringType(), True),
    StructField("PM2.5", FloatType(), True),
    StructField("PM10", FloatType(), True),
    StructField("NO", FloatType(), True),
    StructField("NO2", FloatType(), True),
    StructField("NOx", FloatType(), True),
    StructField("NH3", FloatType(), True),
    StructField("CO", FloatType(), True),
    StructField("SO2", FloatType(), True),
    StructField("O3", FloatType(), True),
    StructField("Benzene", FloatType(), True),
    StructField("Toluene", FloatType(), True),
    StructField("AQI", IntegerType(), True),
    StructField("AQI_Bucket", StringType(), True),
    StructField("StationId", StringType(), True),
    StructField("StationName", StringType(), True),
    StructField("State", StringType(), True),
    StructField("Day_period", StringType(), True),
    StructField("Month", StringType(), True),
    StructField("Year", IntegerType(), True),
    StructField("Season", StringType(), True)
])

# Define the Kafka parameters
kafka_params = {
    "kafka.bootstrap.servers": "localhost:9092",
    "subscribe": "air-quality-1",
    "startingOffsets": "earliest"
}

# Read data from Kafka
kafka_stream_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_params["kafka.bootstrap.servers"]) \
    .option("subscribe", kafka_params["subscribe"]) \
    .option("startingOffsets", kafka_params["startingOffsets"]) \
    .load()

# Deserialize the JSON message from Kafka
parsed_stream_df = kafka_stream_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")
print(parsed_stream_df)
# Define the MongoDB parameters
mongo_db = "air_quality_db"
mongo_collection = "air_quality_collection"

# Write data to MongoDB
mongo_query = parsed_stream_df \
    .writeStream \
    .format("mongodb")\
    .option("database", mongo_db)\
    .option("collection", mongo_collection)\
    .option("checkpointLocation", "./ckpt")\
    .outputMode("append")
#    .foreachBatch(lambda batch_df, batch_id: batch_df.write.format("mongodb").mode("append").option("database", mongo_db).option("collection", mongo_collection).save())
#com.mongodb.spark.sql.DefaultSource
# Start the Spark streaming query
query = mongo_query.start()

# Wait for the query to terminate
query.awaitTermination()

# Consumer configuration
bootstrap_servers = "localhost:9092"
topic_name = "air-quality1"

# Create a Kafka consumer
consumer = KafkaConsumer(topic_name,
                         bootstrap_servers=bootstrap_servers,
                         auto_offset_reset="earliest",
                         value_serializer=lambda x: x.decode('utf-8'))

# Process each message received from Kafka
for message in consumer:
    # Print the message value
    print(message.value)

# Close the consumer
consumer.close()

