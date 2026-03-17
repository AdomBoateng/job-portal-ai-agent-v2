from fastapi import APIRouter, HTTPException
from datetime import datetime

from app.utils.db import get_db
from app.models.models import ScoreRequestPayload, ScoreResponse
from app.helpers.decode64 import decode_base64_text
from app.helpers.logging_config import get_logger
from app.tasks import score_single_cv_task

logger = get_logger("routers.score")
router = APIRouter()


@router.post("/score", response_model=ScoreResponse)
async def receive_score_request(payload: ScoreRequestPayload):
    """Receive a single payload that contains Job + CV for ONE session.

    This endpoint fixes the "JD mismatch" bug by ensuring we store and later fetch
    the Job snapshot by (session_id, job_id) rather than by job_id alone.

    Scoring is dispatched to a Celery worker via Redis for reliable, distributed
    processing that won't crash the FastAPI process under heavy load.
    """
    db = await get_db()
    if db is None:
        logger.error("Database unavailable when receiving score request")
        raise HTTPException(status_code=500, detail="Database Unavailable")

    try:
        logger.info(
            "Receiving score request: session_id=%s job_id=%s application_id=%s",
            payload.session_id,
            payload.job_id,
            payload.application_id,
        )

        # Decode resume content from base64 before storing
        try:
            decoded_text = decode_base64_text(payload.resume)
        except Exception as decode_err:
            logger.error(
                "Failed to decode base64 for CV %s (session %s): %s",
                payload.application_id,
                payload.session_id,
                decode_err,
            )
            raise HTTPException(
                status_code=400, detail="Invalid base64 resume"
            ) from decode_err

        # 1) Upsert job snapshot for THIS session
        job_filter = {"session_id": payload.session_id, "job_id": payload.job_id}
        job_doc = {
            "$set": {
                "session_id": payload.session_id,
                "job_id": payload.job_id,
                "title": payload.title,
                "description": payload.description,
                "skills": payload.skills,
                "responsibilities": payload.responsibilities,
                "updated_at": datetime.now(),
            },
            "$setOnInsert": {"created_at": datetime.now(), "mapped_cvs": []},
        }
        await db["jobs"].update_one(job_filter, job_doc, upsert=True)

        # 2) Insert the CV document for THIS session
        cv_document = {
            "session_id": payload.session_id,
            "application_id": payload.application_id,
            "filename": payload.filename,
            "extracted_text": decoded_text,
            "job_id": payload.job_id,
            "processed": False,
            "score": None,
            "middleware_callback_url": payload.middleware_callback_url,
            "report_sent": False,
            "created_at": datetime.now(),
        }
        insert_result = await db["cvs"].insert_one(cv_document)

        # 3) Map CV to job for THIS session (purely informational)
        await db["jobs"].update_one(
            job_filter,
            {"$addToSet": {"mapped_cvs": payload.application_id}},
        )

        # 4) Dispatch scoring task to Celery worker via Redis
        task = score_single_cv_task.delay(
            application_id=payload.application_id,
            middleware_callback_url=payload.middleware_callback_url,
            cv_id=str(insert_result.inserted_id),
            session_id=payload.session_id,
        )
        logger.info(
            "Dispatched Celery task %s for CV %s", task.id, payload.application_id
        )

        return ScoreResponse(
            session_id=payload.session_id,
            job_id=payload.job_id,
            application_id=payload.application_id,
            message=f"Score request received. Task dispatched (task_id={task.id}).",
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(
            "Error receiving score request session_id=%s application_id=%s: %s",
            payload.session_id,
            payload.application_id,
            e,
            exc_info=True,
        )
        raise HTTPException(status_code=500, detail=str(e))
