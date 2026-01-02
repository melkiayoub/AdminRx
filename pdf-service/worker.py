import pika
import json
import time
from minio import Minio
from datetime import datetime
from weasyprint import HTML
import tempfile

# ---------- MinIO ----------
minio_client = Minio(
    "minio:9000",
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False
)

BUCKET = "pdfs"
if not minio_client.bucket_exists(BUCKET):
    minio_client.make_bucket(BUCKET)

# ---------- PDF Generation ----------
def generate_pdf(data):
    filename = f"{data['name']}.pdf"

    # HTML template with fixed info and dynamic content
    html_content = f"""
    <html>
        <head>
            <style>
                body {{
                    font-family: Arial, sans-serif;
                    color: #333;
                    margin: 40px;
                }}
                h1 {{
                    color: #007BFF;
                }}
                p {{
                    font-size: 14px;
                    line-height: 1.5;
                }}
                .date {{
                    color: #555;
                    font-size: 12px;
                    margin-bottom: 20px;
                }}
                .footer {{
                    margin-top: 50px;
                    font-size: 10px;
                    color: #888;
                }}
            </style>
        </head>
        <body>
            <div class="date">Date: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC</div>
            <h1>{data['name']}</h1>
            <p>{data['content']}</p>
            <div class="footer">This PDF was generated automatically by the PDF Microservice</div>
        </body>
    </html>
    """

    # Use a temporary file to store PDF
    with tempfile.NamedTemporaryFile(suffix=".pdf") as tmp_file:
        HTML(string=html_content).write_pdf(tmp_file.name)
        minio_client.fput_object(BUCKET, filename, tmp_file.name, content_type="application/pdf")

    return filename

# ---------- RabbitMQ (WITH RETRY) ----------
def connect_rabbitmq():
    while True:
        try:
            print("⏳ Connecting to RabbitMQ (pdf-service)...")
            return pika.BlockingConnection(
                pika.ConnectionParameters(host="rabbitmq")
            )
        except pika.exceptions.AMQPConnectionError:
            print("❌ RabbitMQ not ready, retrying in 5 seconds...")
            time.sleep(5)

connection = connect_rabbitmq()
channel = connection.channel()

channel.queue_declare(queue="pdf_requests")
channel.queue_declare(queue="pdf_generated")

# ---------- Consumer ----------
def callback(ch, method, properties, body):
    data = json.loads(body)
    object_name = generate_pdf(data)

    event = {
        "name": data["name"],
        "bucket": BUCKET,
        "object_name": object_name,
        "created_at": datetime.utcnow().isoformat()
    }

    channel.basic_publish(
        exchange="",
        routing_key="pdf_generated",
        body=json.dumps(event)
    )

    ch.basic_ack(delivery_tag=method.delivery_tag)

channel.basic_consume(queue="pdf_requests", on_message_callback=callback)

print("🚀 PDF Service is waiting for messages...")
channel.start_consuming()
##testing
