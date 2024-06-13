import asyncio
from datetime import datetime

import pytz
from motor.motor_asyncio import AsyncIOMotorClient


async def rating_data_preprocessing(mongo_client, data: dict) -> dict | None:

    cur_state = await mongo_client.review.rating.fine_one(
        {"_id": int(data.get("rating_id")), "is_delete": True}
    )
    if cur_state:
        await mongo_client.review.rating.update_one(
            {
                "_id": int(data.get("rating_id")),
            },
            {
                "$set": {
                    "rating": float(data.get("rating")),
                    "last_update": pytz.timezone("Asia/Seoul").localize(
                        datetime.strptime(data.get("last_update"), "%Y-%m-%dT%H:%M:%S")
                    ),
                    "is_delete": False,
                }
            },
        )
        return None
    else:
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


async def preference_data_preprocessing(mongo_client, data: dict) -> dict | None:
    cur_state = await mongo_client.review.rating.fine_one(
        {"_id": int(data.get("preference_id")), "is_delete": True}
    )
    if cur_state:
        await mongo_client.review.rating.update_one(
            {
                "_id": int(data.get("preference_id")),
            },
            {
                "$set": {
                    "last_update": pytz.timezone("Asia/Seoul").localize(
                        datetime.strptime(data.get("last_update"), "%Y-%m-%dT%H:%M:%S")
                    ),
                    "is_delete": False,
                }
            },
        )
        return None
    else:
        return {
            "_id": int(data.get("preference_id")),
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
async def review_delete(
    mongo_client, collection: str, user_id: str, media_id: int, last_update: str
):
    try:
        await mongo_client.review[collection].update_one(
            {"user_id": user_id, "media_id": media_id},
            {
                "$set": {"is_delete": True},
                "last_update": pytz.timezone("Asia/Seoul").localize(
                    datetime.strptime(last_update, "%Y-%m-%dT%H:%M:%S")
                ),
            },
        )
        print(f"삭제 완료")
    except:
        print("삭제 실패")


async def process_member_topic_message(mongo_client: AsyncIOMotorClient, message: dict):
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
        if action == "insert":
            mongo_client.review[data]
            document = (
                await rating_data_preprocessing(mongo_client, data)
                if table == "rating"
                else await preference_data_preprocessing(mongo_client, data)
            )
            if type(document) == dict:
                await mongo_client["review"][table].insert_one(document)
            print(f"Inserted to MongoDB: {document}")
        elif action == "update":
            rating_update(
                mongo_client, data.get("rating_id"), data.get("rating"), data.get("last_update")
            )
            print(f"Updated in MongoDB")
        elif action == "delete":
            # Assuming 'id' is the primary key for the deletion
            # user_document = {"_id": data["user_id"]}
            # review_document = {"user_id": data["user_id"]}
            # update = {"$set": {"is_delete": data["is_delete"]}}
            # await asyncio.gather(
            #     update_is_delete(mongo_client, "member", table, user_document, update),
            #     update_is_delete(mongo_client, "review", "preference", review_document, update),
            #     update_is_delete(mongo_client, "review", "rating", review_document, update),
            # )
            print(f"Deleted from MongoDB")
