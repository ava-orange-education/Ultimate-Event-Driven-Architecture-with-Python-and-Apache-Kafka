import json

from pydantic import BaseModel, ValidationError
from kafka import KafkaProducer

class EventData(BaseModel):
    key: str
    value: dict

# Initialize the Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:29092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

event = {
    'event_id': "12345",
    'event_type': 'User-Registration',
    'data': {'user_id': 99999},
    'timestamp': '2024-10-05T15:23:30',
    'event_name': 'Register-User',
}

try:
    event = EventData(key="user1", value=event)
    producer.send('user.registration', value=event.model_dump())
except ValidationError as e:
    print(f"Validation error: {e}")

producer.flush()
producer.close()
