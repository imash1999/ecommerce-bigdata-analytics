import json
import random
import time
import os
from datetime import datetime
from kafka import KafkaProducer
from faker import Faker
import boto3

fake = Faker()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "ecommerce-events")

print(f"Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS}...")

#  MinIO
s3_client = boto3.client(
    's3',
    endpoint_url='http://minio:9000',
    aws_access_key_id='minioadmin',
    aws_secret_access_key='minioadminpassword'
)

def send_to_minio(event_data):
    try:
        try:
            s3_client.create_bucket(Bucket='raw-events')
        except Exception:
            pass

        file_path = f"events/{datetime.now().strftime('%Y-%m-%d')}/event_{datetime.now().strftime('%H%M%S_%f')}.json"

        s3_client.put_object(
            Bucket='raw-events',
            Key=file_path,
            Body=json.dumps(event_data)
        )
    except Exception as e:
        print(f"Error sending to MinIO: {e}")

# Kafka
producer = None
while not producer:
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
        print("Successfully connected to Kafka!")
    except Exception as e:
        print(f"Waiting for Kafka to be ready... ({e})")
        time.sleep(3)

CATEGORIES = ["electronics", "clothing", "home_decor", "books", "beauty"]
ACTIONS = ["view", "add_to_cart", "buy"]
ACTION_WEIGHTS = [0.7, 0.2, 0.1]

def generate_event():
    action = random.choices(ACTIONS, weights=ACTION_WEIGHTS)[0]
    price = round(random.uniform(5.0, 500.0), 2)

    event = {
        "event_id": fake.uuid4(),
        "user_id": f"user_{random.randint(100, 1000)}",
        "item_id": f"item_{random.randint(1, 50)}",
        "category": random.choice(CATEGORIES),
        "action": action,
        "price": price if action in ["add_to_cart", "buy"] else 0.0,
        "timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    }
    return event

if __name__ == "__main__":
    print(f"Starting event generation to topic: {KAFKA_TOPIC}")
    try:
        while True:
            event = generate_event()
            
           
            producer.send(KAFKA_TOPIC, value=event)
            
            
            send_to_minio(event)
            
            print(f"Sent: {event['action']} | User: {event['user_id']} | Price: {event['price']}")
            time.sleep(random.uniform(0.2, 1.0))
    except KeyboardInterrupt:
        print("Generator stopped.")
        producer.close()
