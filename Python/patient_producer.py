from confluent_kafka import Producer
import json
import time
import random
from datetime import datetime

conf = {
    "bootstrap.servers": "pkc-921jm.us-east-2.aws.confluent.cloud:9092",
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "PLAIN",
    "sasl.username": "GEPIBX66RKECICIE",
    "sasl.password": "cfltqcknspWUx8ascgDp9QoTgVQMzl6tr3CYu2QmZOLQZNGnAfoJt+EUkWzksS+w",
}

producer = Producer(conf)
topic = "Patient_DataPipeline"

def generate_patient_data():
    """Simulate one patient record."""
    return {
        "patient_id": f"P{random.randint(10000, 99999)}",
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "heart_rate": random.randint(60, 100),
        "temperature": round(random.uniform(36.0, 38.0), 1),
        "systolic_bp": random.randint(100, 140),
        "diastolic_bp": random.randint(60, 90),
    }
print("Sending real-time patient data to Kafka...")

try:
    while True:
        record = generate_patient_data()
        producer.produce(topic,key=record["patient_id"],value=json.dumps(record))
        producer.flush()          # ensure it is actually sent
        print("Sent:", record)
        time.sleep(1)             # 1 record per second -> "real time"
except KeyboardInterrupt:
    print("Stopped producer.")