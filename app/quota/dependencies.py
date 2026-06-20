"""
FastAPI dependency injection layer.
quota_guard is the single dependency injected into feature endpoints.
"""

from fastapi import Request, HTTPException
from redis.exceptions import RedisError
from app.quota import service


def quota_guard(feature: str, units: int = 1):
    """
    Factory function that returns a FastAPI dependency.
    Enforces quota before the feature handler executes.

    Usage:
        @router.post("/track-containers")
        async def track_containers(
            _=Depends(quota_guard("container-tracking", units=100))
        ):
            pass
    """

    async def guard(request: Request):
        # extract org_id from request header
        org_id = request.headers.get("X-Org-ID")
        if not org_id:
            raise HTTPException(status_code=400, detail="X-Org-ID header is required")

        # extract idempotency key from request header
        idempotency_key = request.headers.get("X-Idempotency-Key")
        if not idempotency_key:
            raise HTTPException(
                status_code=400, detail="X-Idempotency-Key header is required"
            )

        # get redis and db from app state
        redis = request.app.state.redis
        db = request.app.state.mongo

        try:
            result = await service.consume(
                redis=redis,
                db=db,
                org_id=org_id,
                feature=feature,
                units=units,
                idempotency_key=idempotency_key,
            )
        except RedisError as e:
            raise HTTPException(
                status_code=503, detail="Quota service temporarily unavailable"
            )
        except ValueError as e:
            if str(e) == "org_not_configured":
                raise HTTPException(
                    status_code=403,
                    detail=f"Organization '{org_id}' is not configured for feature '{feature}'",
                )
            raise HTTPException(status_code=500, detail="Internal server error")

        # quota exhausted
        if not result.granted:
            raise HTTPException(
                status_code=429,
                detail={
                    "error": "quota_exhausted",
                    "remaining": result.remaining,
                    "next_reset_at": result.next_reset_at.isoformat(),
                    "org_id": org_id,
                    "feature": feature,
                },
            )

        # quota granted — store result in request state
        # so feature handler can access it if needed
        request.state.quota_result = result

    return guard
