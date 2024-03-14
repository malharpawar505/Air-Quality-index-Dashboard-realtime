from pyspark.sql import SparkSession

spark = SparkSession.builder \
  .appName("KafkaStreamingExample") \
  .getOrCreate()
kafka_df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", bootstrap_servers) \
  .option("subscribe", air-quality-1) \
  .load()
# Add a timestamp to the data if it doesn't already have one
if not "timestamp" in kafka_df.columns:
    kafka_df = kafka_df.withColumn("2024-02-15 10:40:46", F.current_timestamp())

# Process the data as needed
processed_df = kafka_df.select(...)

# Introduce a 15 second delay using windowing
windowed_df = processed_df.window("15 seconds")

# Write the data to a sink (e.g., console, database)
windowed_df.writeStream \
  .format("console") \
  .option("truncate", False) \
  .start()

# Wait for the streaming query to terminate
spark.streams.awaitTermination()
