from kafka import KafkaConsumer, KafkaProducer
import json
import time
from database import SessionLocal
from models import Payment

RETRY_LIMIT = 3  # максимальное количество попыток
RETRY_DELAY = 5  # секунд между попытками

# Consumer с retry и DLQ
while True:
    try:
        consumer = KafkaConsumer(
            "payments",
            bootstrap_servers="kafka:9092",
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            group_id="payment-group",
            auto_offset_reset="earliest",
            enable_auto_commit=True
        )
        break
    except Exception:
        print("Kafka не готов, ждем 5 секунд...")
        time.sleep(5)

print("Kafka подключена, стартуем consumer")

# Producer для DLQ
producer = KafkaProducer(
    bootstrap_servers="kafka:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

for message in consumer:
    data = message.value
    attempts = data.get("attempts", 0)
    db = SessionLocal()

    try:
        payment = db.query(Payment).filter_by(id=data["payment_id"]).first()
        if payment:
            # идемпотентно меняем статус
            if payment.status == "created":
                payment.status = "processing"
            elif payment.status == "processing":
                payment.status = "completed"
            db.commit()
    except Exception as e:
        db.rollback()
        print("Ошибка при обработке:", e)
        if attempts < RETRY_LIMIT:
            # увеличиваем счетчик и отправляем снова в топик payments
            data["attempts"] = attempts + 1
            producer.send("payments", data)
            time.sleep(RETRY_DELAY)
        else:
            # превышен лимит попыток — отправляем в DLQ
            producer.send("payments-dlq", data)
    finally:
        db.close()
