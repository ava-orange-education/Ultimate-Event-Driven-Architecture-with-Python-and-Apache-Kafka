from kafka.coordinator.assignors.sticky.sticky_assignor import StickyPartitionAssignor

import kafka_util

# Common Consumer Config
consumer_config = {
    'bootstrap_servers': 'localhost:9092',
    'security_protocol': 'SSL',
    'ssl_context': kafka_util.ssl_context,
    'auto_offset_reset': 'earliest',
    'enable_auto_commit': False,
    'key_deserializer': kafka_util.string_deserializer,
    'value_deserializer': kafka_util.json_deserializer,
    'max_poll_interval_ms': 420000,
    'partition_assignment_strategy': [StickyPartitionAssignor()]
}
