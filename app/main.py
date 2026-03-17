import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.helpers.logging_config import setup_logging, get_logger
from app.utils.db import get_db
from app.migrations.run_migrations import run_migrations
from app.routers import cv, jd, result, score
from app.routers.task_status import router as task_status_router

# Initialize logging
setup_logging()
logger = get_logger("main")

app = FastAPI(title="CV Screening Service")

cors_origins = [
    origin.strip()
    for origin in os.getenv("CORS_ORIGINS", "*").split(",")
    if origin.strip()
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(result.router, prefix="/api/v2/ai-agent", tags=["Results"])
app.include_router(jd.router, prefix="/api/v2/ai-agent", tags=["Job Descriptions"])
app.include_router(cv.router, prefix="/api/v2/ai-agent", tags=["Curriculum Vitaes"])
app.include_router(score.router, prefix="/api/v2/ai-agent", tags=["Scoring"])
app.include_router(task_status_router, prefix="/api/v2/ai-agent", tags=["Task Status"])


@app.on_event("startup")
async def startup_event():
    """Handle application startup."""
    logger.info("CV Screening Service starting up...")
    # Initialize DB connection pool early
    await get_db()
    # Ensure all MongoDB collections and indexes exist
    db = await get_db()
    if db is not None:
        await run_migrations(db)


@app.on_event("shutdown")
async def shutdown_event():
    """Handle application shutdown."""
    logger.info("CV Screening Service shutting down...")
    from app.utils.db import close_db

    await close_db()


@app.get("/")
async def root():
    logger.debug("Root endpoint accessed")
    return {"message": "AI CV Scoring Service is running", "version": "2.0"}


@app.get("/health")
async def health_check():
    """Check if service, database, and Redis are healthy."""
    health = {"db": "unknown", "redis": "unknown"}

    # Check MongoDB
    try:
        db = await get_db()
        if db is None:
            health["db"] = "unhealthy"
        else:
            health["db"] = "healthy"
    except Exception as e:
        logger.error(f"Health check DB exception: {e}")
        health["db"] = f"unhealthy: {e}"

    # Check Redis
    try:
        import redis as redis_lib

        redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
        r = redis_lib.from_url(redis_url)
        r.ping()
        health["redis"] = "healthy"
    except Exception as e:
        logger.error(f"Health check Redis exception: {e}")
        health["redis"] = f"unhealthy: {e}"

    overall = "healthy" if all(v == "healthy" for v in health.values()) else "unhealthy"
    logger.info(f"Health check: {overall} — {health}")
    return {"status": overall, "components": health}
