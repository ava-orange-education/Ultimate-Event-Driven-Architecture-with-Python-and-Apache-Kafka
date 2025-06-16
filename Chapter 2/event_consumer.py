from kafka import KafkaConsumer
import json

# Initialize Kafka consumer
consumer = KafkaConsumer(
    'user.processed',
    bootstrap_servers='localhost:29092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id=None,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Process each event
for message in consumer:
    event = message.value
    print(f"Consumed event: {event}")
    # Insert event processing logic here, like updating a database or triggering an API call