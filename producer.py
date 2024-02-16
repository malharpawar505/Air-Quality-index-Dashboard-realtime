
import csv
from datetime import datetime
import json
from kafka import KafkaProducer


# Producer configuration
bootstrap_servers = "localhost:9092"
topic_name = "air_quality"

# Open the CSV file
with open("air_quality_index.csv", "r") as csvfile:
    reader = csv.DictReader(csvfile)

    # Create a Kafka producer
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                             value_serializer=lambda x: x.encode('utf-8'))

    # Send each row of data to Kafka with a 15 second delay
    for row in reader:
        # Add a timestamp to the row
        row["2024-02-15 10:40:46"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Convert the row to JSON
        value = json.dumps(row)

        # Send the message to Kafka
        producer.send(topic_name, value)

        # Sleep for 15 seconds
        time.sleep(15)

# Close the producer
producer.close()
