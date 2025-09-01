from kafka import KafkaConsumer

# Initialize Kafka consumer
consumer = KafkaConsumer(
    'user.registration',
    bootstrap_servers='localhost:9092',
    security_protocol='SASL_PLAINTEXT',
    sasl_mechanism='PLAIN',
    sasl_plain_username='alice',
    sasl_plain_password='eda.kafka',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id=None,
    # value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Process each event
for message in consumer:
    event = message.value
    print(f"Consumed event: {event}")