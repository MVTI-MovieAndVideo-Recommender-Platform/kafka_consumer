import json
from datetime import datetime

import pytz
from motor.motor_asyncio import AsyncIOMotorClient


async def rating_data_preprocessing(data: dict) -> dict:
    return {
        "_id": int(data.get("rating_id")),
        "user_id": str(data.get("user_id")),
        "media_id": int(data.get("media_id")),
        "rating": float(data.get("rating")),
        "last_update": pytz.timezone("Asia/Seoul").localize(
            datetime.strptime(data.get("last_update"), "%Y-%m-%dT%H:%M:%S")
        ),
        "is_delete": data.get("is_delete", False),
    }


async def preference_data_preprocessing(data: dict) -> dict:
    print(f"preference_data_preprocessing -> {data}")
    return {
        "_id": data.get("preference_id"),
        "user_id": str(data.get("user_id")),
        "media_id": int(data.get("media_id")),
        "last_update": pytz.timezone("Asia/Seoul").localize(
            datetime.strptime(data.get("last_update"), "%Y-%m-%dT%H:%M:%S")
        ),
        "is_delete": data.get("is_delete", False),
    }


# 별점 업데이트
async def rating_update(mongo_client, rating_id: int, rating: float, last_update):
    try:
        await mongo_client.review.rating.update_one(
            {"_id": rating_id},
            {
                "$set": {
                    "rating": rating,
                    "last_update": pytz.timezone("Asia/Seoul").localize(
                        datetime.strptime(last_update, "%Y-%m-%dT%H:%M:%S")
                    ),
                }
            },
        )
        print(f"rating 업데이트 완료")
    except:
        print("rating 업데이트 실패")


# 소프트 삭제
async def review_is_delete_state(
    mongo_client, collection: str, _id: int, last_update: str, is_delete: bool
):
    try:
        await mongo_client.review[collection].update_one(
            {"_id": _id},
            {
                "$set": {
                    "is_delete": is_delete,
                    "last_update": pytz.timezone("Asia/Seoul").localize(
                        datetime.strptime(last_update, "%Y-%m-%dT%H:%M:%S")
                    ),
                }
            },
        )
        print(f"수정 완료")
    except:
        print("수정 실패")


async def process_review_topic_message(mongo_client: AsyncIOMotorClient, message: str):
    message = json.loads(message)
    print(f"type : {type(message)} and {message}")
    action = None
    data = None

    if "insert" in message:
        action = "insert"
        data = message["insert"]
    elif "update" in message:
        action = "update"
        data = message["update"]
    elif "delete" in message:
        action = "delete"
        data = message["delete"]

    if data:
        table = "rating" if "rating" in data else "preference"
        data = data.get(table)
        print(data)
        if action == "insert":
            document = (
                await rating_data_preprocessing(data)
                if table == "rating"
                else await preference_data_preprocessing(data)
            )
            await mongo_client["review"][table].insert_one(document)
            print(f"Inserted to MongoDB: {document}")
        elif action == "update":
            rating_update(
                mongo_client, data.get("rating_id"), data.get("rating"), data.get("last_update")
            )
            print(f"Updated in MongoDB")
        elif action == "delete":
            if table == "rating":
                await review_is_delete_state(
                    mongo_client,
                    "rating",
                    data.get("rating_id"),
                    data.get("last_update"),
                    data.get("is_delete"),
                )
            else:
                await review_is_delete_state(
                    mongo_client,
                    "preference",
                    data.get("preference_id"),
                    data.get("last_update"),
                    data.get("is_delete"),
                )
            print(f"Deleted from MongoDB")
