import csv
import json
import os
import time
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

KAFKA_BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
TOPIC = "transactions.raw"
CSV_PATH = os.getenv("PAYSIM_CSV", "data/raw/dirty_PS_20174392719_1491204439457_log.csv")
PRODUCE_DELAY = float(os.getenv("PRODUCE_DELAY", "0.05"))


def create_producer(retries=10, delay=5):
    for attempt in range(retries):
        try:
            return KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                retries=3,
            )
        except NoBrokersAvailable:
            print(f"Kafka not ready, retrying in {delay}s... (attempt {attempt+1}/{retries})")
            time.sleep(delay)
    raise RuntimeError("Could not connect to Kafka after multiple retries")


def stream_transactions():
    producer = create_producer()
    print(f"Connected to Kafka at {KAFKA_BROKER}")

    with open(CSV_PATH, "r") as f:
        reader = csv.DictReader(f)
        count = 0
        for row in reader:
            message = {
                "step": float(row["step"]),
                "type": row["type"],
                "amount": float(row["amount"]),
                "nameOrig": row["nameOrig"],
                "oldbalanceOrg": float(row["oldbalanceOrg"]),
                "newbalanceOrig": float(row["newbalanceOrig"]),
                "nameDest": row["nameDest"],
                "oldbalanceDest": float(row["oldbalanceDest"]),
                "newbalanceDest": float(row["newbalanceDest"]),
                "isFraud": float(row["isFraud"]),
                "isFlaggedFraud": float(row["isFlaggedFraud"]),
            }
            producer.send(TOPIC, value=message)
            count += 1

            if count % 1000 == 0:
                print(f"Sent {count} transactions...")

            time.sleep(PRODUCE_DELAY)

        producer.flush()
        producer.close()
        print(f"Done. Sent {count} transactions to '{TOPIC}'")


if __name__ == "__main__":
    stream_transactions()
