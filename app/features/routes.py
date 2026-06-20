"""
Feature endpoints — demonstrate quota engine in action.
Each endpoint is protected by quota_guard dependency.
"""

from fastapi import APIRouter, Depends, Request, HTTPException
from fastapi.routing import APIRouter
from pydantic import BaseModel, Field
from app.quota.models import QuotaResult, UsageResponse
from app.quota.dependencies import quota_guard
from app.quota import service

router = APIRouter()


# ── Request Models ─────────────────────────────────────────────────────────────


class TrackContainersPayload(BaseModel):
    containers: list[str] = Field(..., min_length=1)


class SailingSchedulePayload(BaseModel):
    origin_port: str
    destination_port: str
    lookups: int = Field(default=1, gt=0)


class RefundPayload(BaseModel):
    org_id: str
    feature: str
    units: int = Field(..., gt=0)
    idempotency_key: str


class QuotaConfigPayload(BaseModel):
    org_id: str
    feature: str
    limit: int = Field(..., gt=0)


# ── Feature Endpoints ──────────────────────────────────────────────────────────


@router.post("/track-containers")
async def track_containers(
    request: Request,
    payload: TrackContainersPayload,
):
    """
    Track a batch of containers.
    Consumes len(containers) quota units atomically.
    """
    units = len(payload.containers)

    # manually call quota_guard with dynamic units
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
    """
    Look up sailing schedules.
    Consumes payload.lookups quota units.
    """
    await quota_guard(feature="sailing-schedule", units=payload.lookups)(request)

    return {
        "status": "found",
        "origin": payload.origin_port,
        "destination": payload.destination_port,
        "units_consumed": payload.lookups,
        "quota_remaining": request.state.quota_result.remaining,
    }


# ── Quota Management Endpoints ─────────────────────────────────────────────────


@router.get("/quota/usage")
async def quota_usage(
    request: Request,
    org_id: str,
    feature: str,
) -> UsageResponse:
    """
    Returns current quota usage for an org and feature.
    """
    redis = request.app.state.redis
    db = request.app.state.mongo

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
    payload: RefundPayload,
) -> QuotaResult:
    """
    Refunds quota units back to an org.
    Called when downstream operation fails after quota was deducted.
    """
    redis = request.app.state.redis

    try:
        return await service.refund(
            redis=redis,
            org_id=payload.org_id,
            feature=payload.feature,
            units=payload.units,
            idempotency_key=payload.idempotency_key,
        )
    except ValueError:
        raise HTTPException(
            status_code=403, detail=f"Organization '{payload.org_id}' is not configured"
        )


@router.post("/admin/quota/config")
async def quota_config(
    request: Request,
    payload: QuotaConfigPayload,
) -> dict:
    """
    Sets quota limit for an org and feature.
    Writes to both Redis and MongoDB simultaneously.
    """
    redis = request.app.state.redis
    db = request.app.state.mongo

    config_key = f"quota_config:{payload.org_id}:{payload.feature}"

    # write to Redis — no TTL, permanent
    await redis.set(config_key, payload.limit)

    # write to MongoDB — source of truth
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
