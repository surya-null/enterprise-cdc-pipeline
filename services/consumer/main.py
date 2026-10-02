import json
import logging
from typing import Optional, Dict, Any
from pydantic import BaseModel, Field
from kafka import KafkaConsumer

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("CDC-Sink")

class OrderPayload(BaseModel):
    order_id: str
    customer_id: str
    total_amount: float
    status: str
    currency: str

class DebeziumEnvelope(BaseModel):
    op: str = Field(..., description="c=create, u=update, d=delete, r=read")
    before: Optional[OrderPayload] = None
    after: Optional[OrderPayload] = None
    ts_ms: Optional[int] = None

def process_stream():
    consumer = KafkaConsumer(
        'cdc.sales.orders',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='order-sync-pipeline-v1',
        value_deserializer=lambda v: json.loads(v.decode('utf-8'))
    )
    
    logger.info("Subscribed to 'cdc.sales.orders'. Listening for database changes...")

    for message in consumer:
        raw_val = message.value
        payload = raw_val.get('payload', raw_val)

        try:
            event = DebeziumEnvelope(**payload)
            if event.op == 'c':
                logger.info(f"[CREATE] Order Created: {event.after.order_id} | Amount: ${event.after.total_amount}")
            elif event.op == 'u':
                logger.info(f"[UPDATE] Order Modified: {event.after.order_id} | Status: {event.before.status} -> {event.after.status}")
            elif event.op == 'd':
                logger.warning(f"[DELETE] Order Deleted: {event.before.order_id}")
            elif event.op == 'r':
                logger.info(f"[SNAPSHOT] Initial State Synced: {event.after.order_id}")
        except Exception as e:
            logger.error(f"Malformed schema received: {e}")

if __name__ == "__main__":
    process_stream()
    