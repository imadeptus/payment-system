import pika

connection = pika.BlockingConnection(
    pika.ConnectionParameters('localhost')
)
channel = connection.channel()

channel.queue_declare(queue='test_queue', durable=True)

channel.basic_publish(
    exchange='',
    routing_key='test_queue',
    body='Hello Rabbit'
)

print("Sent")

connection.close()