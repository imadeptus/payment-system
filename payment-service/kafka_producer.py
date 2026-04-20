from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers="kafka:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def send_payment_event(payment_id, amount):
    producer.send(
        "payments",
        {
            "payment_id": payment_id,
            "amount": amount
        }
    )
    producer.flush()
