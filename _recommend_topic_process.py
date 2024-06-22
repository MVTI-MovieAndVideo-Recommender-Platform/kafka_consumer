import json
from datetime import datetime

import pytz
from motor.motor_asyncio import AsyncIOMotorClient


async def recommend_data_preprocessing(data: dict) -> dict:
    print(f"recommend_data_preprocessing -> {data}")
    print(
        pytz.timezone("Asia/Seoul").localize(
            datetime.strptime(data.get("recommendation_time"), "%Y-%m-%dT%H:%M:%S")
        )
    )
    return {
        "_id": int(data.get("recommendation_id")),
        "user_id": data.get("user_id"),
        "user_mbti": data.get("user_mbti"),
        "input_media_id": [int(media_id) for media_id in data.get("input_media_id").split(", ")],
        "recommended_media_id": [
            int(media_id) for media_id in data.get("recommended_media_id").split(", ")
        ],
        "recommendation_time": pytz.timezone("Asia/Seoul").localize(
            datetime.strptime(data.get("recommendation_time"), "%Y-%m-%dT%H:%M:%S")
        ),
        "re_recommendation": data.get("re_recommendation"),
    }


async def process_recommend_topic_message(mongo_client: AsyncIOMotorClient, message: str):
    message = json.loads(message)
    print(f"type : {type(message)} and {message}")

    data = message["insert"]
    table = "recommendation"
    data = data.get(table)
    document = await recommend_data_preprocessing(data)
    await mongo_client["recommend"][table].insert_one(document)
    print(f"Inserted to MongoDB: {document}")
