from confluent_kafka import Consumer, KafkaError

# Kafka consumer configuration
conf = {
    'bootstrap.servers': 'localhost:9092',  # Replace with your Kafka broker(s) address
    'group.id': 'my-consumer-group',  # Consumer group ID
    'auto.offset.reset': 'earliest'  # Read from the beginning of the topic
}

consumer = Consumer(conf)

topic = 'my-topic'  # Replace with the name of your Kafka topic
consumer.subscribe([topic])

try:
    while True:
        msg = consumer.poll(1.0)  # Poll for messages, waiting up to 1 second

        if msg is None:
            continue

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print(f"Reached end of partition {msg.partition()}")
            else:
                print(f"Error while consuming from topic {msg.topic()}: {msg.error()}")
        else:
            # Print the received message key and value
            print(f"Received message: key={msg.key()}, value={msg.value()}")

except KeyboardInterrupt:
    print("Consumer interrupted.")
finally:
    # Close the consumer when finished
    consumer.close()
