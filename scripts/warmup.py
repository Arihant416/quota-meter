"""
Warmup script — loads all quota configs from MongoDB into Redis.
Run this:
  - On first deployment
  - After Redis restart
  - On demand via: python -m scripts.warmup
"""

import asyncio
import logging
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.core.config import create_redis, create_mongo, DB_NAME

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def warmup():
    redis = create_redis()
    mongo = create_mongo()
    db = mongo[DB_NAME]

    try:
        # verify connections
        await redis.ping()
        logger.info("✅ Redis connected")

        await mongo.admin.command("ping")
        logger.info("✅ MongoDB connected")

        await db["quota_configs"].create_index(
            [("org_id", 1), ("feature", 1)], unique=True
        )
        logger.info("✅ MongoDB index ensured")

        # fetch all configs from MongoDB
        configs = await db["quota_configs"].find({}).to_list(length=None)

        if not configs:
            logger.warning("⚠️  No quota configs found in MongoDB")
            logger.warning("⚠️  Use POST /api/v1/admin/quota/config to add configs")
            return

        # load into Redis using pipeline for speed
        pipe = redis.pipeline()
        for config in configs:
            # FIX (Flaw A): Enforce curly braces around org_id to match cluster alignment patterns in store.py
            key = f"quota_config:{{{config['org_id']}}}:{config['feature']}"
            pipe.set(key, config["limit"])  # no TTL — permanent

        await pipe.execute()

        logger.info(f"✅ Warmed {len(configs)} quota configs into Redis")

    except Exception as e:
        logger.error(f"❌ Warmup failed: {e}")
        raise

    finally:
        await redis.aclose()
        mongo.close()
        logger.info("🔴 Connections closed")


if __name__ == "__main__":
    asyncio.run(warmup())
