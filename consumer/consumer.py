from confluent_kafka import Consumer

consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'test',
    'auto.offset.reset': 'earliest'
})

consumer.subscribe(['events-2'])

print("Waiting for messages...")

while True:
    message = consumer.poll(1.0)
    if message is None:
        continue

    if message.error():
        print(f"Error on topic {message.topic()}: {message.error()}")
        continue

    print(
        f"Message from topic {message.topic()}: {message.value().decode('utf-8')}"
        f" (partition={message.partition()}, offset={message.offset()})"
    )
