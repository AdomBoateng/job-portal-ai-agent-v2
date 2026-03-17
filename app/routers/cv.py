from fastapi import APIRouter, HTTPException
from datetime import datetime
from app.utils.db import get_db
from app.models.models import CVResponse, CVPayload
from app.tasks import score_single_cv_task, batch_score_cvs_task
from app.helpers.decode64 import decode_base64_text
from app.helpers.logging_config import get_logger

logger = get_logger("routers.cv")

router = APIRouter()


@router.post("/applications", response_model=CVResponse)
async def receive_cv(payload: CVPayload):
    """
    Receive a CV from middleware application.
    Stores CV in MongoDB with auto-generated ID.
    Dispatches a Celery scoring task via Redis for reliable background processing.
    """
    try:
        logger.info(
            f"Receiving CV: {payload.application_id} - {payload.filename} for job: {payload.job_id}"
        )
        db = await get_db()
        if db is None:
            logger.error("Database unavailable when receiving CV")
            raise HTTPException(status_code=500, detail="Database Unavailable")

        # Decode CV content from base64 before storing
        try:
            decoded_text = decode_base64_text(payload.resume)
        except Exception as decode_err:
            logger.error(
                f"Failed to decode base64 for CV {payload.application_id}: {decode_err}"
            )
            raise HTTPException(
                status_code=400, detail="Invalid base64 for extracted_text"
            ) from decode_err

        preview = decoded_text[:100].encode("unicode_escape").decode("ascii")
        logger.info(f"Resume content: {preview}...")
        logger.debug(f"Successfully decoded CV text: {len(decoded_text)} characters")

        cv_document = {
            "application_id": payload.application_id,
            "filename": payload.filename,
            "extracted_text": decoded_text,
            "job_id": payload.job_id,
            "processed": False,
            "score": None,
            "middleware_callback_url": payload.middleware_callback_url,
            "report_sent": False,
            "session_id": payload.session_id,
            "created_at": datetime.now(),
        }

        insert_result = await db["cvs"].insert_one(cv_document)
        logger.info(f"Successfully stored CV: {payload.application_id}")

        # If CV is linked to a Job, update the Job's mapped_cvs array
        if payload.job_id:
            logger.debug(
                f"Attempting to add CV {payload.application_id} to job {payload.job_id}"
            )
            result = await db["jobs"].update_one(
                {"job_id": payload.job_id},
                {"$addToSet": {"mapped_cvs": payload.application_id}},
            )
            if result.modified_count > 0:
                logger.debug(
                    f"Successfully added CV {payload.application_id} to job {payload.job_id} mapped_cvs"
                )
            else:
                logger.warning(
                    f"Job {payload.job_id} not found or CV already in mapped_cvs for CV {payload.application_id}"
                )
        else:
            logger.warning(f"CV {payload.application_id} received without job_id")

        # Dispatch scoring task to Celery worker via Redis
        logger.info(f"Dispatching Celery scoring task for CV: {payload.application_id}")
        task = score_single_cv_task.delay(
            application_id=payload.application_id,
            middleware_callback_url=payload.middleware_callback_url,
            cv_id=str(insert_result.inserted_id),
            session_id=payload.session_id,
        )
        logger.info(f"Celery task {task.id} dispatched for CV {payload.application_id}")

        return CVResponse(
            application_id=payload.application_id,
            filename=payload.filename,
            message=f"CV '{payload.filename}' received. Scoring dispatched (task_id={task.id}).",
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error receiving CV {payload.application_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/status/{application_id}")
async def get_cv_status(application_id: str):
    """
    Check the screening status of a specific CV.
    Returns: processing, completed, or error.
    """
    try:
        logger.debug(f"Checking status for CV: {application_id}")
        db = await get_db()
        if db is None:
            logger.error("Database unavailable when checking CV status")
            raise HTTPException(status_code=500, detail="Database Unavailable")

        cv_cursor = (
            db["cvs"]
            .find({"application_id": application_id})
            .sort([("created_at", -1), ("_id", -1)])
        )
        cv_list = await cv_cursor.to_list(length=1)
        cv = cv_list[0] if cv_list else None
        if not cv:
            logger.warning(f"CV not found: {application_id}")
            raise HTTPException(
                status_code=404, detail=f"CV {application_id} not found"
            )

        status = "completed" if cv.get("processed") else "processing"
        logger.info(f"CV {application_id} status: {status}, score: {cv.get('score')}")

        return {
            "application_id": application_id,
            "filename": cv.get("filename"),
            "status": status,
            "score": cv.get("score"),
            "report_sent": cv.get("report_sent", False),
            "created_at": cv.get("created_at"),
            "report_sent_at": cv.get("report_sent_at"),
        }
    except Exception as e:
        logger.error(f"Error checking CV status for {application_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/screen-cvs")
async def screen_cvs():
    """
    Manually trigger batch screening of unprocessed CVs via Celery.
    Note: CVs are now auto-screened when received via /receive-cv.
    This endpoint is useful for re-screening or batch processing.
    """
    logger.info("Manual batch CV screening triggered via Celery")
    task = batch_score_cvs_task.delay()
    return {
        "message": "Batch CV screening dispatched to Celery worker",
        "task_id": task.id,
        "status": "dispatched",
        "note": "CVs received via /applications are already auto-screened",
    }
