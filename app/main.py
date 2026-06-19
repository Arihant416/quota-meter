import logging
from fastapi import FastAPI
from contextlib import asynccontextmanager
from app.core.config import create_redis, create_mongo

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # this will be on startup
    app.state.redis = create_redis()
    app.state.mongo = create_mongo()

    try:
        await app.state.redis.ping()
        logger.info("✅ Redis connection successful")
    except Exception as e:
        logger.error(f"❌ Redis connection failed: {e}")
        raise

    try:
        await app.state.mongo.admin.command("ping")
        logger.info("✅ Mongo connection successful")
    except Exception as e:
        logger.error(f"❌ Mongo connection failed: {e}")
        raise

    yield

    # on shutdown
    await app.state.redis.aclose()
    app.state.mongo.close()
    logger.info("🔴 Connections closed")


app = FastAPI(title="Quota Metering Engine", version="1.0.0", lifespan=lifespan)


@app.get("/health")
async def health():
    return {"status": "ok"}
