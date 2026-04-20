# rabbit_producer.py
import pika, json

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
channel.queue_declare(queue='payments')

def send_payment_event(payment):
    channel.basic_publish(
        exchange='',
        routing_key='payments',
        body=json.dumps(payment)
    )
    print("Sent to RabbitMQ:", payment)