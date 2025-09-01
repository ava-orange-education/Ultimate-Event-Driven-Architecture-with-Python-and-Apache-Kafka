from kafka import KafkaProducer
import json

# Initialize the Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:29092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Create a sample event
event = {
    'event_id': "12345",
    'event_type': 'User-Registration',
    'data': {'user_id': 99999},
    'timestamp': '2024-10-05T15:23:30',
    'event_name': 'Register-User',
}

# Send the event to a Kafka topic named 'user.registration'
producer.send('user.registration', event)

producer.flush()
# Optional: Close the producer
producer.close()

print("Event sent to Kafka!")