import json
import logging
from datetime import time

import pybreaker
from kafka import KafkaProducer
from kafka.errors import KafkaError

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

def send_to_dead_letter_queue(producer, key, value, error):
    dlq_event = {"event": value, "error": str(error)}
    producer.send('dead.letter', key=key, value=json.dumps(dlq_event).encode('utf-8'))

def send_with_retry(producer, topic, key, value, retries=3):
    for attempt in range(retries):
        try:
            producer.send(topic, key=key, value=value).get(timeout=100)
            producer.flush()
            logger.debug(f"Event: {value} sent to kafka topic: {topic}")
            return  # Successful send
        except KafkaError as e:
            logger.error(f"Attempt {attempt + 1} failed: {e}")
            # print(f"Attempt {attempt + 1} failed: {e}")
            time.sleep(2 ** attempt)  # Exponential backoff
    # raise Exception("All retries failed")
    logger.debug("All retries failed, sending to dlq")
    send_to_dead_letter_queue(producer, key, value, "All retries failed")

circuit_breaker = pybreaker.CircuitBreaker(fail_max=5, reset_timeout=30)

@circuit_breaker
def produce_event(producer, topic, key, value):
    send_with_retry(producer, topic, key, value)

# Initialize the Kafka producer
event_producer = KafkaProducer(
    bootstrap_servers='localhost:29092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=str.encode
)

# Create a sample event
event = {
    'event_type': 'User-Registration',
    'user_id': 100000,
    'timestamp': '2024-12-05T15:23:30',
    'event_name': 'Register-User',
}

produce_event(event_producer, "user.registration", key="user_1", value=event)
