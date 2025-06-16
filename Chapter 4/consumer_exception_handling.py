import json
import logging
import sqlite3
import time
from datetime import datetime

import pybreaker
from kafka import KafkaConsumer, KafkaProducer
from psycopg import DatabaseError
from pydantic import BaseModel, ValidationError

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Initialize database connection
conn = sqlite3.connect('events.db')
cursor = conn.cursor()

# Create table if not exists
cursor.execute('''
CREATE TABLE IF NOT EXISTS processed_events (
    event_id TEXT PRIMARY KEY,
    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
    metadata TEXT
)
''')
conn.commit()

class EventSchema(BaseModel):
    key: str
    value: dict

def is_duplicate(event_id):
    cursor.execute("SELECT event_id FROM processed_events WHERE event_id = ?", (event_id,))
    return cursor.fetchone() is not None

def process_event(event):
    logger.debug(f"Consumed event: {event}")
    event_id = event["event_id"]

    # Insert event processing logic here, like updating a database or triggering an API call
    if is_duplicate(event_id):
        print(f"Duplicate event detected: {event_id}")
        return
    # Process the event (your logic here)
    cursor.execute("INSERT INTO processed_events (event_id, metadata) VALUES (?, ?)",
                   (event_id, json.dumps(event)))
    conn.commit()
    logger.debug(f"Event processed: {event_id}")

def validate_event(key, event):
    try:
        validated_event = EventSchema(key=key, value=event)
        return validated_event
    except ValidationError as e:
        logger.error(f"Event validation failed: {e}")
        send_to_dead_letter_queue(event, reason="ValidationError")

def send_to_dead_letter_queue(key, event, reason):
    dlq_event = {
        "original_event": event,
        "reason": reason,
        "timestamp": datetime.now().isoformat(),
    }
    event_producer.send('dead.letter', value=dlq_event, key=key)
    event_producer.flush()
    event_producer.close()

def retry_event(event, retries=3, backoff=2):
    for attempt in range(retries):
        try:
            process_event(event)
            return
        except DatabaseError:
            time.sleep(backoff ** attempt)
    # Send to DLQ if retries fail
    send_to_dead_letter_queue(event, reason="MaxRetriesExceeded")

circuit_breaker = pybreaker.CircuitBreaker(fail_max=5, reset_timeout=30)

@circuit_breaker
def process_with_circuit_breaker(event):
    process_event(event)

def consume_event(key, event):
    try:
        validated_event = validate_event(key, event)
        value = json.loads(validated_event.model_dump_json())["value"]
        process_with_circuit_breaker(value)  # Core event processing logic
    except ValidationError as e:
        logger.error(f"Validation error: {e}")
        send_to_dead_letter_queue(key, value, reason="ValidationError")
    except DatabaseError as e:
        logger.error(f"Database error: {e}")
        retry_event(value)
    except Exception as e:
        logger.critical(f"Unhandled error: {e}")
        send_to_dead_letter_queue(key, value, reason=str(e))

# Initialize Kafka consumer
event_consumer = KafkaConsumer(
    'user.registration',
    bootstrap_servers='localhost:29092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id=None,
    # value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Initialize the Kafka producer
event_producer = KafkaProducer(
    bootstrap_servers='localhost:29092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=str.encode
)

# Process each event
for message in event_consumer:
    event = json.loads(message.value.decode('ascii'))
    key = event["key"]
    value = event["value"]
    consume_event(key, value)