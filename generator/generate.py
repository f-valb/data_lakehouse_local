"""
E-commerce event generator.
Produces `orders` and `clicks` events to Kafka at ~EVENTS_PER_SEC rate.
"""
import json, os, random, time, uuid
from datetime import datetime, timezone
from faker import Faker
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")
EVENTS_PER_SEC  = float(os.getenv("EVENTS_PER_SEC", "2"))
SLEEP           = 1.0 / EVENTS_PER_SEC

fake = Faker()
random.seed()

PRODUCTS = [
    {"id": f"P{i:04d}", "name": fake.catch_phrase(), "category": random.choice(
        ["Electronics","Clothing","Books","Home","Sports","Beauty","Toys"]),
     "price": round(random.uniform(4.99, 499.99), 2)}
    for i in range(1, 201)
]

def make_order():
    product = random.choice(PRODUCTS)
    qty     = random.randint(1, 5)
    return {
        "event_type":   "order",
        "order_id":     str(uuid.uuid4()),
        "user_id":      f"U{random.randint(1, 5000):05d}",
        "session_id":   str(uuid.uuid4()),
        "product_id":   product["id"],
        "product_name": product["name"],
        "category":     product["category"],
        "quantity":     qty,
        "unit_price":   product["price"],
        "total_amount": round(product["price"] * qty, 2),
        "currency":     "USD",
        "status":       random.choices(
            ["completed","pending","cancelled","refunded"],
            weights=[70, 15, 10, 5])[0],
        "payment_method": random.choice(["credit_card","paypal","apple_pay","bank_transfer"]),
        "country":      fake.country_code(),
        "timestamp":    datetime.now(timezone.utc).isoformat(),
        "kafka_key":    str(uuid.uuid4()),
    }

def make_click():
    product = random.choice(PRODUCTS)
    return {
        "event_type":   "click",
        "click_id":     str(uuid.uuid4()),
        "user_id":      f"U{random.randint(1, 5000):05d}",
        "session_id":   str(uuid.uuid4()),
        "product_id":   product["id"],
        "product_name": product["name"],
        "category":     product["category"],
        "page":         random.choice(["home","search","category","product","cart","checkout"]),
        "referrer":     random.choice(["google","facebook","email","direct","instagram",None]),
        "device":       random.choice(["desktop","mobile","tablet"]),
        "duration_ms":  random.randint(500, 120000),
        "converted":    random.random() < 0.08,
        "timestamp":    datetime.now(timezone.utc).isoformat(),
    }

def get_producer(retries=20, delay=5):
    for attempt in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode(),
                key_serializer=lambda k: k.encode() if k else None,
            )
            print(f"✅ Connected to Kafka at {KAFKA_BOOTSTRAP}", flush=True)
            return producer
        except NoBrokersAvailable:
            print(f"⏳ Waiting for Kafka... attempt {attempt+1}/{retries}", flush=True)
            time.sleep(delay)
    raise RuntimeError("Could not connect to Kafka")

def main():
    producer = get_producer()
    count = 0
    while True:
        # alternate between orders and clicks with 30/70 split
        if random.random() < 0.30:
            event = make_order()
            topic = "orders"
            key   = event.get("order_id")
        else:
            event = make_click()
            topic = "clicks"
            key   = event.get("click_id")

        producer.send(topic, value=event, key=key)
        count += 1
        if count % 20 == 0:
            producer.flush()
            print(f"📤 Sent {count} events", flush=True)
        time.sleep(SLEEP)

if __name__ == "__main__":
    main()
