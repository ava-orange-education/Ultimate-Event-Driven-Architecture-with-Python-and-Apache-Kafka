from confluent_kafka import Producer

producer = Producer({
    'bootstrap.servers': 'localhost:29092',
    'transactional.id': '100'
})

# Initialize the producer's transaction
producer.init_transactions()

try:
    # Begin a transaction
    producer.begin_transaction()

    # Send messages as part of the transaction
    producer.produce('user.registration', key='key_1', value='registration1')
    producer.produce('user.registration', key='key_2', value='registration2')

    # Commit the transaction if all operations succeed
    producer.commit_transaction()
except Exception as e:
    print(f"Transaction failed: {e}")
    # Abort the transaction on error
    producer.abort_transaction()
