from confluent_kafka import Consumer
import json
import requests
import time
from datetime import datetime
import pandas as pd


conf = {
    "bootstrap.servers": "pkc-921jm.us-east-2.aws.confluent.cloud:9092",
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "PLAIN",
    "sasl.username": "GEPIBX66RKECICIE",
    "sasl.password": "cfltqcknspWUx8ascgDp9QoTgVQMzl6tr3CYu2QmZOLQZNGnAfoJt+EUkWzksS+w",
    "group.id": "python-consumer-group",
    "auto.offset.reset": "earliest",
}

consumer = Consumer(conf)
topic = "Patient_DataPipeline"

consumer.subscribe([topic])
print("Listening for real-time patient data...")


# -------- 2. VALIDATION FUNCTION --------
def validate(record: dict) -> bool:
    required_fields = [
        "patient_id", "timestamp",
        "heart_rate", "temperature",
        "systolic_bp", "diastolic_bp",
    ]

    # Check missing fields
    for field in required_fields:
        if field not in record:
            print(f"❌ Missing field: {field}")
            return False

    # Check value ranges (custom logic)
    if not (30 <= record["heart_rate"] <= 200):
        print("❌ Invalid heart rate:", record["heart_rate"])
        return False

    if not (35.0 <= record["temperature"] <= 42.0):
        print("❌ Invalid temperature:", record["temperature"])
        return False

    if not (60 <= record["systolic_bp"] <= 200):
        print("❌ Invalid systolic_bp:", record["systolic_bp"])
        return False

    if not (40 <= record["diastolic_bp"] <= 130):
        print("❌ Invalid diastolic_bp:", record["diastolic_bp"])
        return False

    return True

# -------- 3. TRANSFORMATION FUNCTION --------
def transform(record: dict) -> dict:
    rec = record.copy()
    rec["heart_rate"] = round(float(rec["heart_rate"]), 2)
    rec["temperature"] = round(float(rec["temperature"]), 2)
    rec["processed_at"] = datetime.utcnow().isoformat()
    rec["ingest_time"] = datetime.utcnow().isoformat() + "Z"
    rec["is_high_bp"] = (
        rec["systolic_bp"] >= 140 or rec["diastolic_bp"] >= 90
    )
    return rec

# -------- 4. HDFS CONFIG + WRITER --------
HDFS_IP = "192.168.1.36"
NAMENODE_PORT = "50070"
HDFS_DIR = "/healthflow/patient_data/"
BATCH_SIZE = 10

def write_to_hdfs_csv(batch):
    """Write a batch of records as CSV into HDFS (date-partitioned)."""
    if not batch:
        print("   (empty batch, nothing to write)")
        return

    df = pd.DataFrame(batch)
    csv_data = df.to_csv(index=False)

    today = datetime.utcnow().strftime("%Y-%m-%d")
    filename = f"patient_{int(time.time())}.csv"
    hdfs_path = f"/healthflow/patient_data/date={today}/{filename}"

    url = (
        f"http://{HDFS_IP}:{NAMENODE_PORT}/webhdfs/v1"
        f"{hdfs_path}?op=CREATE&overwrite=true"
    )
    print(f"📁 Uploading CSV to HDFS: {hdfs_path}")

    try:
        # Step 1 – get redirect URL
        step1 = requests.put(url, allow_redirects=False, timeout=10)
        if "Location" not in step1.headers:
            print("❌ No redirect URL from HDFS:", step1.text)
            return

        upload_url = step1.headers["Location"]

        # Step 2 – upload CSV
        step2 = requests.put(upload_url, data=csv_data, timeout=30)

        if step2.status_code == 201:
            print("✅ CSV stored in HDFS successfully\n")
        else:
            print("❌ Failed to write CSV:", step2.status_code, step2.text)
    except Exception as e:
        print("HDFS error:", repr(e))

batch = []
try:
    while True:
        msg = consumer.poll(5.0)

        if msg is None:
            print("  (no message yet...)")
            continue

        if msg.error():
            print("Consumer error:", msg.error())
            continue

        # Step 1: decode JSON from Kafka
        try:
            data = json.loads(msg.value().decode("utf-8"))
        except Exception as e:
            print("JSON decode error:", e)
            continue

        print("📥 Raw Kafka record:", data)

        # Step 2: VALIDATION
        if not validate(data):
            print("Invalid record, skipping:", data, "\n")
            continue

        # Step 3: TRANSFORMATION
        transformed = transform(data)
        print("✅ Valid, transformed record:", transformed)

        # Step 4: ADD TO BATCH
        batch.append(transformed)
        if len(batch) >= BATCH_SIZE:
            write_to_hdfs_csv(batch)
            batch = []  # clear batch



except KeyboardInterrupt:
    print("Stopped consumer.")
finally:
    write_to_hdfs_csv(batch)
    consumer.close()