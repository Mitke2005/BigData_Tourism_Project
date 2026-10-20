import uuid
import json
import traceback
from kafka import KafkaConsumer

print("Testing KafkaConsumer...")
serving_group = f"web_serving_{uuid.uuid4().hex[:8]}"
try:
    consumer = KafkaConsumer(
        'tourism-output',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        group_id=serving_group,
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        api_version=(2, 8, 1),
        consumer_timeout_ms=5000
    )
    print("Created consumer, iterating...")
    for msg in consumer:
        print("Got msg key:", msg.key, "action:", msg.value.get('action') if isinstance(msg.value, dict) else None)
        break
    consumer.close()
    print("Success!")
except Exception as e:
    traceback.print_exc()
