from confluent_kafka import Producer
import asyncio

from prompt_toolkit import PromptSession
from prompt_toolkit.patch_stdout import patch_stdout


producer = Producer({
    'bootstrap.servers': 'localhost:9092'
})


def delivery_report(err, msg):
    if err:
        print(f"Message delivery failed: {err}")
        return

    print(
        f"Message delivered to topic {msg.topic()}"
        f" (partition={msg.partition()}, offset={msg.offset()})"
    )


async def kafka_loop():
    while True:
        producer.poll(0)
        await asyncio.sleep(0.01)


async def input_loop():
    session = PromptSession()

    with patch_stdout():
        while True:
            text = await session.prompt_async("Enter message: ")

            if text in ["exit", "quit", ""]:
                break

            producer.produce(
                "events-2",
                value=text.encode("utf-8"),
                callback=delivery_report
            )


async def main():
    kafka_task = asyncio.create_task(kafka_loop())

    try:
        await input_loop()
    finally:
        kafka_task.cancel()
        producer.flush()


asyncio.run(main())
