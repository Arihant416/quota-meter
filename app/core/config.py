import os
from redis.asyncio import Redis
from motor.motor_asyncio import AsyncIOMotorClient # a pymongo alternative for async features battle tested

# ------ Redis, Mongo and Db name extracted from ENV vars-----
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
MONGO_URL = os.getenv("MONGO_URL", "mongodb://localhost:27017")
DB_NAME = os.getenv("DB_NAME", "quota_db")


# ------ connection factories------
def create_redis() -> Redis:
    return Redis.from_url(REDIS_URL, encoding="utf-8", decode_responses=True)


def create_mongo() -> AsyncIOMotorClient:
    return AsyncIOMotorClient(MONGO_URL, maxPoolSize=50, minPoolSize=8)


# ------accessors to be used with (Depends()) -> A Dependency Injection mechanism by FastAPI.
async def get_redis(request) -> Redis:
    return request.app.state.redis


async def get_mongo_db(request):
    return request.app.state.mongo[DB_NAME]
