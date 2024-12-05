from kafka import KafkaConsumer
from json import loads, JSONDecodeError
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import Config

def consume_messages_from_kafka():
   
    # Create Kafka Consumer
    print("Creating Kafka consumer")
    consumer = KafkaConsumer(
        Config.KAFKA_TOPIC,  # Topic name
        bootstrap_servers=[Config.KAFKA_BROKER],  
        auto_offset_reset='earliest', 
        enable_auto_commit=True, 
        group_id='ev_charging_consumer_group',  
        value_deserializer=lambda x: loads(x.decode('utf-8')) if x else None 
    )

    print(f"Subscribed to Kafka topic: {Config.KAFKA_TOPIC}")
    print(f"Connected to Kafka broker: {Config.KAFKA_BROKER}")

    for message in consumer:
        try:
            print(f"Raw message: {message.value}") 

            if message.value is not None:
                print(f"Received message: {message.value}")
            else:
                print("Received an empty or null message.")
        except JSONDecodeError as e:
            print(f"Failed to decode message: {e}")
            print(f"Problematic message: {message.value}")
        except Exception as e:
            print(f"Unexpected error: {e}")