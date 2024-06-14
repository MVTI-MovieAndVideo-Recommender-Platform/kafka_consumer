from datetime import datetime

import pytz
from motor.motor_asyncio import AsyncIOMotorClient


# 문자열을 datetime 객체로 변환하는 함수 (시, 분, 초를 0으로 설정)
def string_to_datetime(date_str):
    try:
        # 시간 정보가 없는 경우
        return datetime.strptime(date_str, "%Y-%m-%d").replace(hour=0, minute=0, second=0)
    except ValueError:
        # 날짜와 시간이 모두 있는 경우
        return datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S")


async def process_content_topic_message(mongo_client: AsyncIOMotorClient, message: str):
    print(f"Received message: {message}")

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
    ) = message.split("@#$")

    if certification == "None" or not certification:
        certification = None
    if genre == "None" or not genre:
        genre = None
    if origin_country == "None" or not origin_country:
        origin_country = None
    if director == "None" or not director:
        director = None
    if actor == "None" or not actor:
        actor = None
    if platform == "None" or not platform:
        platform = None

    release_date = pytz.timezone("Asia/Seoul").localize(string_to_datetime(release_date))
    mongo_document = {
        "_id": int(id),
        "title": title,
        "runtime": int(runtime),
        "release_date": release_date,
        "certification": certification.split(", ")[0] if type(certification) == str else None,
        "genre": [g for g in genre.split(", ") if g] if type(genre) == str else None,
        "origin_country": origin_country.split(", ") if type(origin_country) == str else None,
        "overview": overview,
        "director": director.split(", ") if type(director) == str else None,
        "actor": actor.split(", ") if type(actor) == str else None,
        "platform": platform.split(", ") if type(platform) == str else None,
        "rating_value": float(rating_value),
        "rating_count": int(rating_count),
        "posterurl_count": int(posterurl_count),
        "backdropurl_count": int(backdropurl_count),
        "posterurl": posterurl.split(", ") if type(posterurl) == str else None,
        "backdropurl": backdropurl.split(", ") if type(backdropurl) == str else None,
    }

    await mongo_client.content["media"].insert_one(mongo_document)
    print(f"Inserted to MongoDB: {mongo_document}")
