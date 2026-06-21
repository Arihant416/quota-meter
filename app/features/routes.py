"""
Feature endpoints — demonstrate quota engine in action.
Each endpoint is protected by quota_guard dependency.
"""

from fastapi import HTTPException, Request
from fastapi.routing import APIRouter
from pydantic import BaseModel, Field

from app.core.config import DB_NAME
from app.quota import service
from app.quota.dependencies import quota_guard
from app.quota.models import QuotaResult, RefundRequest, UsageResponse

router = APIRouter()


class TrackContainersPayload(BaseModel):
    containers: list[str] = Field(..., min_length=1)


class SailingSchedulePayload(BaseModel):
    origin_port: str
    destination_port: str
    lookups: int = Field(default=1, gt=0)


class QuotaConfigPayload(BaseModel):
    org_id: str
    feature: str
    limit: int = Field(..., gt=0)


@router.post("/track-containers")
async def track_containers(
    request: Request,
    payload: TrackContainersPayload,
):
    units = len(payload.containers)
    await quota_guard(feature="container-tracking", units=units)(request)

    return {
        "status": "tracked",
        "containers": payload.containers,
        "units_consumed": units,
        "quota_remaining": request.state.quota_result.remaining,
    }


@router.post("/sailing-schedule")
async def sailing_schedule(
    request: Request,
    payload: SailingSchedulePayload,
):
    await quota_guard(feature="sailing-schedule", units=payload.lookups)(request)

    return {
        "status": "found",
        "origin": payload.origin_port,
        "destination": payload.destination_port,
        "units_consumed": payload.lookups,
        "quota_remaining": request.state.quota_result.remaining,
    }


@router.get("/quota/usage")
async def quota_usage(
    request: Request,
    org_id: str,
    feature: str,
) -> UsageResponse:
    redis = request.app.state.redis
    db = request.app.state.mongo[DB_NAME]

    try:
        return await service.get_usage(redis, db, org_id, feature)
    except ValueError:
        raise HTTPException(
            status_code=403,
            detail=f"Organization '{org_id}' is not configured for feature '{feature}'",
        )


@router.post("/quota/refund")
async def quota_refund(
    request: Request,
    payload: RefundRequest,
) -> QuotaResult:
    redis = request.app.state.redis

    try:
        return await service.refund(
            redis=redis,
            org_id=payload.org_id,
            feature=payload.feature,
            original_idempotency_key=payload.original_idempotency_key,
        )
    except ValueError as e:
        detail = str(e)

        if detail == "org_not_configured":
            raise HTTPException(
                status_code=403, detail="Organization is not configured"
            )
        if detail == "original_request_not_found":
            raise HTTPException(status_code=404, detail="Original request not found")
        if detail == "request_org_feature_mismatch":
            raise HTTPException(
                status_code=400,
                detail="Original request does not belong to supplied org/feature",
            )
        if detail == "cannot_refund_denied_request":
            raise HTTPException(
                status_code=400,
                detail="Cannot refund a denied quota request",
            )

        raise HTTPException(status_code=400, detail=detail)


@router.post("/admin/quota/config")
async def quota_config(
    request: Request,
    payload: QuotaConfigPayload,
) -> dict:
    redis = request.app.state.redis
    db = request.app.state.mongo[DB_NAME]

    # FIX (Flaw A): Enforce curly braces around org_id to match cluster alignment patterns in store.py
    config_key = f"quota_config:{{{payload.org_id}}}:{payload.feature}"

    # Write to Redis hot store
    await redis.set(config_key, payload.limit)

    # Write to Mongo source of truth
    await db["quota_configs"].update_one(
        {"org_id": payload.org_id, "feature": payload.feature},
        {
            "$set": {
                "org_id": payload.org_id,
                "feature": payload.feature,
                "limit": payload.limit,
            }
        },
        upsert=True,
    )

    return {
        "status": "configured",
        "org_id": payload.org_id,
        "feature": payload.feature,
        "limit": payload.limit,
    }
