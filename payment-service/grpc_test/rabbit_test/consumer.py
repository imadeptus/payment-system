import pika
import time

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

channel.queue_declare(queue='test_queue', durable=True)

def callback(ch, method, properties, body):
    print("Received:", body.decode())

    # имитируем долгую обработку
    time.sleep(5)

    # вручную подтверждаем
    ch.basic_ack(delivery_tag=method.delivery_tag)
    print("Message acknowledged")

channel.basic_consume(
    queue='test_queue',
    on_message_callback=callback,
    auto_ack=False  # ВАЖНО
)

print("Waiting for messages...")
channel.start_consuming()