from datetime import datetime

import pytz
from motor.motor_asyncio import AsyncIOMotorClient


# 문자열을 datetime 객체로 변환하는 함수 (시, 분, 초를 0으로 설정)
def string_to_datetime(date_string):
    date_part = datetime.strptime(date_string, "%Y-%m-%d")
    return datetime(date_part.year, date_part.month, date_part.day, 0, 0, 0)


async def process_content_topic_message(mongo_client: AsyncIOMotorClient, message: str):
    print(f"Received message: {message.value}")

    # 메시지에서 데이터를 추출
    (
        id,
        title,
        runtime,
        release_date,
        certification,
        genre,
        origin_country,
        overview,
        director,
        actor,
        platform,
        rating_value,
        rating_count,
        posterurl_count,
        backdropurl_count,
        posterurl,
        backdropurl,
    ) = message.value.split("@#$")

    release_date = pytz.timezone("Asia/Seoul").localize(
        datetime.strptime(string_to_datetime(release_date), "%Y-%m-%dT%H:%M:%S")
    )
    mongo_document = {
        "id": int(id),
        "title": title,
        "runtime": int(runtime),
        "release_date": release_date,
        "certification": certification.split(", ")[0] if type(certification) == str else "",
        "genre": genre.split(", ") if type(genre) == str else "",
        "origin_country": origin_country.split(", ") if type(origin_country) == str else "",
        "overview": overview,
        "director": director.split(", ") if type(director) == str else "",
        "actor": actor.split(", ") if type(actor) == str else "",
        "platform": platform.split(", "),
        "rating_value": float(rating_value),
        "rating_count": int(rating_count),
        "posterurl_count": int(posterurl_count),
        "backdropurl_count": int(backdropurl_count),
        "posterurl": posterurl.split(", ") if type(posterurl) == str else "",
        "backdropurl": backdropurl.split(", ") if type(backdropurl) == str else "",
    }

    await mongo_client.content["media"].insert_one(mongo_document)
    print(f"Inserted to MongoDB: {mongo_document}")
