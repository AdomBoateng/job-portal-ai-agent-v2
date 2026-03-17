import os
from celery import Celery
from dotenv import load_dotenv

load_dotenv()

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

celery_app = Celery(
    "cv_screening",
    broker=REDIS_URL,
    backend=REDIS_URL,
)

celery_app.conf.update(
    # Serialization
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],

    # Reliability: acknowledge tasks AFTER completion so they survive worker restarts
    task_acks_late=True,
    worker_prefetch_multiplier=1,

    # Concurrency – replaces the old asyncio.Semaphore(MAX_CONCURRENT_SCORING)
    worker_concurrency=int(os.getenv("CELERY_CONCURRENCY", "5")),

    # Result expiry (keep results for 24 hours for middleware polling)
    result_expires=86400,

    # Celery 6 requires this to keep retrying broker connections during worker startup.
    broker_connection_retry_on_startup=True,

    # Task discovery
    include=["app.tasks"],

    # Timezone
    timezone="UTC",
    enable_utc=True,
)
