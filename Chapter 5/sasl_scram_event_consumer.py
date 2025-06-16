from kafka import KafkaConsumer
import ssl

# SSL/TLS configuration
ssl_context = ssl.create_default_context(
    purpose=ssl.Purpose.SERVER_AUTH,
    cafile='/var/kafka/ssl/ca-cert.crt'
)
ssl_context.load_cert_chain(
    certfile='/var/kafka/ssl/eda.client.crt',
    keyfile='/var/kafka/ssl/eda.client.key'
)

ssl_context.check_hostname = False

# Initialize Kafka consumer
consumer = KafkaConsumer(
    'user.registration',
    bootstrap_servers='localhost:9094',
    security_protocol='SASL_SSL',
    sasl_mechanism='SCRAM-SHA-256',
    ssl_context=ssl_context,
    sasl_plain_username='alice',
    sasl_plain_password='eda_kafka',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id=None,
    # value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Process each event
for message in consumer:
    event = message.value
    print(f"Consumed event: {event}")