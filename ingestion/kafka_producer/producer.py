import json
import os
import time
import uuid
import random
from datetime import datetime, timezone
from kafka import KafkaProducer

# -----------------------------
# Configuration
# -----------------------------
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:29092")
TOPIC = os.getenv("KAFKA_TOPIC", "product.events.v1")

EVENT_TYPES = ["page_view", "checkout", "payment", "api_call", "error"]
FEATURES = ["search", "cart", "checkout", "profile"]
COUNTRIES = ["IN", "US", "DE", "SG"]
OS_TYPES = ["android", "ios", "web"]

# -----------------------------
# Kafka Producer
# -----------------------------
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    key_serializer=lambda k: k.encode("utf-8"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks="all",
    retries=3,
)

# -----------------------------
# Event Generator
# -----------------------------
def generate_event(anomaly=False):
    user_id = f"user_{random.randint(1, 500)}"
    now = datetime.now(timezone.utc)

    latency = random.randint(50, 300)

    # Controlled anomaly
    if anomaly:
        latency = random.randint(2000, 5000)

    return user_id, {
        "event_id": str(uuid.uuid4()),
        "event_type": random.choice(EVENT_TYPES),
        "user_id": user_id,
        "session_id": str(uuid.uuid4()),
        "event_ts": now.isoformat(),
        "ingest_ts": datetime.now(timezone.utc).isoformat(),
        "product": {
            "feature": random.choice(FEATURES),
            "action": "click"
        },
        "device": {
            "os": random.choice(OS_TYPES),
            "app_version": "1.0.0"
        },
        "geo": {
            "country": random.choice(COUNTRIES),
            "region": "NA"
        },
        "metrics": {
            "latency_ms": latency,
            "value": round(random.random() * 100, 2)
        }
    }

# -----------------------------
# Main Loop
# -----------------------------
if __name__ == "__main__":
    print("🚀 PulseIQ Producer Started")

    counter = 0

    while True:
        # Inject anomaly every 25 events
        anomaly = counter % 25 == 0 and counter != 0

        key, event = generate_event(anomaly=anomaly)

        producer.send(TOPIC, key=key, value=event)

        if anomaly:
            print(f"⚠️ Anomaly injected for user {key}")
        else:
            print(f"✅ Event sent for user {key}")

        counter += 1
        time.sleep(0.5)

