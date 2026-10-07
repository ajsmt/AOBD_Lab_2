import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

TOPIC_NAME = "events-topic"
BOOTSTRAP_SERVERS = ["localhost:9092"]

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: str(k).encode("utf-8"),
)

users = [f"user_{i}" for i in range(1, 10)]
actions = ["click", "view", "purchase", "login", "logout"]

try:
    while True:
        now = datetime.now()
        user_id = random.choice(users)
        event = {
            "user_id": user_id,
            "action": random.choice(actions),
            "timestamp": now.strftime("%Y-%m-%d %H:%M:%S"),
            "timestamp_ms": int(time.time() * 1000),
            "value": random.randint(1, 100),
        }
        producer.send(TOPIC_NAME, key=user_id, value=event)
        print(f"[-> Sent]: {event}")
        time.sleep(random.uniform(0.2, 0.5))

finally:
    producer.flush()
    producer.close()