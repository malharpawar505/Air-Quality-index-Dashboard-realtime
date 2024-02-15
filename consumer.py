from kafka import KafkaConsumer

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
