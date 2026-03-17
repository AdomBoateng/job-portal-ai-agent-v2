from fastapi import APIRouter, HTTPException
from celery.result import AsyncResult

from app.celery_app import celery_app
from app.helpers.logging_config import get_logger

logger = get_logger("routers.task_status")

router = APIRouter()


@router.get("/task-status/{task_id}")
async def get_task_status(task_id: str):
    """
    Query the status of a Celery scoring task.

    States: PENDING → STARTED → SUCCESS | FAILURE | RETRY
    The middleware can poll this endpoint as an alternative to callbacks.
    """
    try:
        result = AsyncResult(task_id, app=celery_app)

        response = {
            "task_id": task_id,
            "state": result.state,
        }

        if result.state == "SUCCESS":
            response["result"] = result.result
        elif result.state == "FAILURE":
            response["error"] = str(result.result)
        elif result.state == "STARTED":
            response["info"] = result.info if result.info else "Task is running"

        return response
    except Exception as e:
        logger.error(f"Error checking task status {task_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))
