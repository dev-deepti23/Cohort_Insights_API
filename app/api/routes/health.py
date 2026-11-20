from fastapi import APIRouter, Request, status
from fastapi.responses import JSONResponse

router = APIRouter(tags=["health"])


@router.get("/health")
async def health_check(request: Request) -> JSONResponse:
    """Verify MongoDB and Redis connectivity."""
    mongo_ok = await request.app.state.mongo_manager.ping()
    redis_ok = await request.app.state.redis_manager.ping()

    overall_ok = mongo_ok and redis_ok
    http_status = status.HTTP_200_OK if overall_ok else status.HTTP_503_SERVICE_UNAVAILABLE
    return JSONResponse(
        status_code=http_status,
        content={
            "status": "ok" if overall_ok else "degraded",
            "dependencies": {
                "mongodb": "up" if mongo_ok else "down",
                "redis": "up" if redis_ok else "down",
            },
        },
    )