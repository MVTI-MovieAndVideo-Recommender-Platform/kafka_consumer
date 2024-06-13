import asyncio
from datetime import datetime

import pytz
from motor.motor_asyncio import AsyncIOMotorClient


async def user_data_preprocessing(data: dict) -> dict:
    print(type(data.get("last_update")), data.get("last_update"))
    return {
        "_id": data.get("user_id"),
        "name": data.get("name"),
        "email": data.get("email"),
        "gender": data.get("gender"),
        "birthyear": data.get("birthyear"),
        "mbti": data.get("mbti", None),
        "last_update": pytz.timezone("Asia/Seoul").localize(
            datetime.strptime(data.get("last_update"), "%Y-%m-%dT%H:%M:%S")
        ),
        "is_delete": data.get("is_delete", False),
    }


async def auth_data_preprocessing(data: dict) -> dict:
    # 인증 데이터 전처리 로직
    return {
        "_id": data.get("token"),
        "provider": data.get("provider"),
        "created_at": pytz.timezone("Asia/Seoul").localize(
            datetime.strptime(data["created_at"], "%Y-%m-%dT%H:%M:%S")
        ),
    }


async def update_is_delete(mongo_client, db_name, collection_name, column, update):
    collection = mongo_client[db_name][collection_name]
    result = await collection.update_many(column, update)
    print(f"Updated {result.modified_count} documents in {db_name}.{collection_name}.")


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
        table = "auth" if "auth" in data else "user"
        data = data.get(table)
        if action == "insert":
            document = (
                await user_data_preprocessing(data)
                if table == "user"
                else await auth_data_preprocessing(data)
            )
            await mongo_client["member"][table].insert_one(document)
            print(f"Inserted to MongoDB: {document}")
        elif action == "update":
            # Assuming 'id' is the primary key for the update
            document = {"_id": data["user_id"]}
            update = {
                "$set": {
                    "mbti": data["mbti"],
                    "last_update": pytz.timezone("Asia/Seoul").localize(
                        datetime.strptime(data["last_update"], "%Y-%m-%dT%H:%M:%S")
                    ),
                }
            }
            await mongo_client["member"][table].update_one(document, update)
            print(f"Updated in MongoDB: {document}")
        elif action == "delete":
            # Assuming 'id' is the primary key for the deletion
            user_document = {"_id": data["user_id"]}
            review_document = {"user_id": data["user_id"]}
            update = {"$set": {"is_delete": data["is_delete"]}}
            await asyncio.gather(
                update_is_delete(mongo_client, "member", table, user_document, update),
                update_is_delete(mongo_client, "review", "preference", review_document, update),
                update_is_delete(mongo_client, "review", "rating", review_document, update),
            )
            print(f"Deleted from MongoDB")
