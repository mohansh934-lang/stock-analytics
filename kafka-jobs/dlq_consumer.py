from kafka import KafkaConsumer
import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

consumer = KafkaConsumer(
    'stocks_dlq',
    bootstrap_servers='localhost:9092',
    group_id='dlq_consumer_group',
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode())
)

logger.info("DLQ Consumer started")

for msg in consumer:
    error_data = msg.value
    logger.error(
        f"DLQ RECORD | Symbol: {error_data.get('symbol')} | "
        f"Reason: {error_data.get('error')}"
    )

    # yahan:
    # - alert bhej sakta hai
    # - file me dump
    # - DB me store
