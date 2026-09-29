"""Generator data order dummy -> Kafka topic."""
import json, os, random, time
from kafka import KafkaProducer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("TOPIC", "orders")
RATE = float(os.getenv("RATE_PER_SEC", "5"))
CITIES = ["Jakarta", "Bandung", "Surabaya", "Medan", "Makassar"]

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

print(f"Producing to {TOPIC} @ {BOOTSTRAP} ({RATE}/s)")
n = 0
while True:
    producer.send(TOPIC, {
        "id": random.randint(1, 10**9),
        "city": random.choice(CITIES),
        "amount": random.randint(10, 500),
        "ts": time.time(),
    })
    n += 1
    if n % 50 == 0:
        producer.flush()
        print(f"sent {n} events")
    time.sleep(1 / RATE)
