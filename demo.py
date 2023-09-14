from kafka_producer import send_json_message

# Example JSON data
json_data = {
    "name": "Dummy User",
    "email": "dummy.user@thermondo-fake-email.de",
    "age": 30
}

# Name of the Kafka topic to send the JSON data to
topic = "my-topic"


# Send the JSON data to the Kafka topic
send_json_message(topic, json_data)