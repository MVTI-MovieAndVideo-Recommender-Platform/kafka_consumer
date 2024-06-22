import asyncio
import json
import os

from confluent_kafka import Consumer, KafkaError, KafkaException
from dotenv import load_dotenv
from motor.motor_asyncio import AsyncIOMotorClient

from _content_topic_process import process_content_topic_message
from _member_topic_process import process_member_topic_message
from _recommend_topic_process import process_recommend_topic_message
from _review_topic_process import process_review_topic_message

load_dotenv()


async def consume_messages(consumer, mongo_client, topics):
    consumer.subscribe(topics)

    while True:
        msg = consumer.poll(0)
        if msg is None:
            await asyncio.sleep(0.2)  # 메시지가 없을 경우 0.2초 대기
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                raise KafkaException(msg.error())

        message_value = msg.value().decode("utf-8-sig")
        # message_dict = json.loads(message_value)

        topic = msg.topic()

        if topic == "member-topic":
            await process_member_topic_message(mongo_client, message_value)
        elif topic == "content-topic":
            await process_content_topic_message(mongo_client, message_value)
        elif topic == "review-topic":
            await process_review_topic_message(mongo_client, message_value)
        elif topic == "recommend-topic":
            await process_recommend_topic_message(mongo_client, message_value)


async def main():
    conf = {
        "bootstrap.servers": os.environ.get("kafka_host"),
        "group.id": "my_group",
        "auto.offset.reset": "earliest",
    }
    consumer = Consumer(**conf)
    # MongoDB 클라이언트 설정
    mongo_client = AsyncIOMotorClient(os.environ.get("mongo_host"))
    print(os.environ.get("kafka_host"))
    print(os.environ.get("mongo_host"))
    topics = ["content-topic", "member-topic", "review-topic", "recommend-topic"]
    await consume_messages(consumer, mongo_client, topics)


if __name__ == "__main__":
    print("consumer start...")
    asyncio.run(main())
