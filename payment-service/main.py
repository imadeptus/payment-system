from fastapi import FastAPI, Header, HTTPException
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from database import SessionLocal, Base, engine
from models import Payment
from kafka_producer import send_payment_event

app = FastAPI()

# создаём таблицы
Base.metadata.create_all(bind=engine)


@app.post("/payments")
def create_payment(amount: int, idempotency_key: str = Header(...)):
    db = SessionLocal()

    try:
        query = """
        INSERT INTO payments (id, amount, idempotency_key, status)
        VALUES (gen_random_uuid(), :amount, :idempotency_key, 'created')
        ON CONFLICT (idempotency_key) DO UPDATE
        SET amount = payments.amount
        RETURNING id, status, idempotency_key;
        """

        result = db.execute(
            text(query),
            {
                "amount": amount,
                "idempotency_key": idempotency_key
            }
        ).first()

        db.commit()

        if not result:
            raise HTTPException(status_code=500, detail="Insert failed")

        result_dict = dict(result._mapping)

        # Kafka не должен ломать API
        try:
            send_payment_event(result_dict["id"], amount)
        except Exception as kafka_error:
            print("Kafka error:", kafka_error)

        return result_dict

    except SQLAlchemyError as db_error:
        db.rollback()
        print("DB error:", db_error)
        raise HTTPException(status_code=500, detail="Database error")

    finally:
        db.close()
