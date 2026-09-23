from kafka import KafkaConsumer
import json
import logging
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_consumer():
    kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    consumer = KafkaConsumer(
        'stock_prices',              # topic
        bootstrap_servers=kafka_servers,
        auto_offset_reset='earliest', # pehle se padhe
        enable_auto_commit=True,
        group_id='stock_consumer_group_v2',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    return consumer


def consume_messages():
    consumer = create_consumer()
    logger.info("Kafka Consumer started...")

    for message in consumer:
        data = message.value
        logger.info(
            f"Received | Symbol: {data['symbol']} | "
            f"Close: {data['close']} | Volume: {data['volume']}"
        )

if __name__ == "__main__":
    consume_messages()


