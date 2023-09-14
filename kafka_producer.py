from confluent_kafka import Producer
import json

# Kafka producer configuration
_CONF = {
    'bootstrap.servers': 'localhost:9092',  # Replace with your Kafka broker(s) address
    'client.id': 'my-producer'
}


def get_producer():
    return Producer(_CONF)


def send_json_message(topic, json_data):
    producer = get_producer()
    try:
        # Produce the JSON message to the specified topic
        producer.produce(topic, key=None, value=json.dumps(json_data))
        producer.flush()  # Ensure all messages are sent

        print(f"Sent JSON message to topic {topic}: {json_data}")
    except Exception as e:
        print(f"Error sending message: {str(e)}")
