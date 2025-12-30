import pika
import json
import psycopg2
import time

# ---------- PostgreSQL (WITH RETRY) ----------
def connect_postgres():
    while True:
        try:
            print("⏳ Connecting to PostgreSQL (storage-service)...")
            return psycopg2.connect(
                host="postgres",
                dbname="pdfdb",
                user="pdfuser",
                password="pdfpass"
            )
        except psycopg2.OperationalError:
            print("❌ PostgreSQL not ready, retrying in 5 seconds...")
            time.sleep(5)

db = connect_postgres()
cur = db.cursor()

cur.execute("""
CREATE TABLE IF NOT EXISTS pdf_files (
    id SERIAL PRIMARY KEY,
    name TEXT,
    bucket TEXT,
    object_name TEXT,
    created_at TIMESTAMP
)
""")
db.commit()

# ---------- RabbitMQ (WITH RETRY) ----------
def connect_rabbitmq():
    while True:
        try:
            print("⏳ Connecting to RabbitMQ (storage-service)...")
            return pika.BlockingConnection(
                pika.ConnectionParameters(host="rabbitmq")
            )
        except pika.exceptions.AMQPConnectionError:
            print("❌ RabbitMQ not ready, retrying in 5 seconds...")
            time.sleep(5)

rabbit = connect_rabbitmq()
channel = rabbit.channel()

channel.queue_declare(queue="pdf_generated")
channel.queue_declare(queue="pdf_stored")

# ---------- Consumer ----------
def callback(ch, method, properties, body):
    data = json.loads(body)

    cur.execute(
        "INSERT INTO pdf_files (name, bucket, object_name, created_at) VALUES (%s, %s, %s, %s)",
        (data["name"], data["bucket"], data["object_name"], data["created_at"])
    )
    db.commit()

    channel.basic_publish(
        exchange="",
        routing_key="pdf_stored",
        body=json.dumps(data)
    )

    ch.basic_ack(delivery_tag=method.delivery_tag)

print("🚀 Storage Service is waiting for messages...")
channel.basic_consume(queue="pdf_generated", on_message_callback=callback)
channel.start_consuming()
