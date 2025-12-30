from fastapi import FastAPI
import pika, json
from prometheus_fastapi_instrumentator import Instrumentator

app = FastAPI()
Instrumentator().instrument(app).expose(app)

@app.post("/generate-pdf")
def generate_pdf(payload: dict):
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host="rabbitmq")
    )
    channel = connection.channel()
    channel.queue_declare(queue="pdf_requests")

    channel.basic_publish(
        exchange="",
        routing_key="pdf_requests",
        body=json.dumps(payload)
    )

    connection.close()
    return {"status": "PDF request sent"}
##some comment here to test if it push to dockerhub or no on push to main
