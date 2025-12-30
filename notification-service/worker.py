import pika
import json
import time

# ---------- RabbitMQ (WITH RETRY) ----------
def connect_rabbitmq():
    while True:
        try:
            print("⏳ Connecting to RabbitMQ (notification-service)...")
            return pika.BlockingConnection(
                pika.ConnectionParameters(host="rabbitmq")
            )
        except pika.exceptions.AMQPConnectionError:
            print("❌ RabbitMQ not ready, retrying in 5 seconds...")
            time.sleep(5)

connection = connect_rabbitmq()
channel = connection.channel()
channel.queue_declare(queue="pdf_stored")

# ---------- Consumer ----------
def callback(ch, method, properties, body):
    data = json.loads(body)
    print(f"📢 PDF READY: {data['name']} stored in MinIO")
    ch.basic_ack(delivery_tag=method.delivery_tag)

print("🚀 Notification Service is waiting for messages...")
channel.basic_consume(queue="pdf_stored", on_message_callback=callback)
channel.start_consuming()
#####test