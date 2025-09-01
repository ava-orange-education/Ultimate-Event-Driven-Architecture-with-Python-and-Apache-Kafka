from kafka import KafkaConsumer, KafkaProducer
import json

# Initialize Kafka consumer to read from the 'events_processor' topic
consumer = KafkaConsumer(
    'user.registration',
    bootstrap_servers='localhost:29092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Initialize Kafka producer to send to the 'events_processed' topic
producer = KafkaProducer(
    bootstrap_servers='localhost:29092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Process events
for message in consumer:
    raw_event = message.value
    processed_event = {
        'user_id': raw_event['user_id'],
        'event_type': 'user-registration-processed',
        'processed_timestamp': '2024-10-05T12:00:00'
    }

    # Send processed event to the 'events_processed' topic
    producer.send('user.processed', processed_event)

# Close the connections
consumer.close()
producer.close()

print("Events processed and published!")