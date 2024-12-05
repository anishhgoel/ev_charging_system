#utility funtions
from kafka import KafkaProducer
from json import dumps
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import Config
producer = KafkaProducer(
    bootstrap_servers=[Config.KAFKA_BROKER],
    value_serializer=lambda v: dumps(v).encode('utf-8')
    )


#sendind message to topic defined in cConfig
def send_message_to_kafka(message):
    topic = Config.KAFKA_TOPIC
    producer.send(topic, value= message)
    # Ensure all messages are sent before exiting
    producer.flush()
    print(f"Message sent to topic {topic}: {message}")

if __name__ == "__main__":
    message = {"charger_id": "CH-1", "status": "charging", "power": 20.5, "timestamp": 1701708990}
    send_message_to_kafka(message)