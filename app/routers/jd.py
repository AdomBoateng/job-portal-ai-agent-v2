from fastapi import APIRouter, HTTPException
from datetime import datetime

from app.utils.db import get_db
from app.models.models import JDResponse, JDPayload
from app.helpers.logging_config import get_logger

logger = get_logger("routers.jd")
router = APIRouter()


@router.post("/jobs", response_model=JDResponse)
async def receive_jd(payload: JDPayload):
    """Receive a Job Description from middleware.

    Job snapshots are scoped by (session_id, job_id) when a session is present.
    Legacy requests without session_id fall back to job_id-only upserts.
    """
    try:
        logger.info(f"Receiving JD: {payload.job_id} - {payload.title} (session_id={payload.session_id})")
        db = await get_db()
        if db is None:
            logger.error("Database unavailable when receiving JD")
            raise HTTPException(status_code=500, detail="Database Unavailable")

        job_filter = {"job_id": payload.job_id}
        if payload.session_id is not None:
            job_filter["session_id"] = payload.session_id

        jd_update = {
            "$set": {
                "job_id": payload.job_id,
                "title": payload.title,
                "description": payload.description,
                "skills": payload.skills,
                "responsibilities": payload.responsibilities,
                "session_id": payload.session_id,
                "updated_at": datetime.now(),
            },
            "$setOnInsert": {"mapped_cvs": [], "created_at": datetime.now()},
        }

        await db["jobs"].update_one(job_filter, jd_update, upsert=True)
        logger.info(f"Stored/updated JD: job_id={payload.job_id} session_id={payload.session_id}")

        return JDResponse(
            job_id=payload.job_id,
            title=payload.title,
            message=f"JD '{payload.title}' received and stored successfully",
        )
    except Exception as e:
        logger.error(f"Error receiving JD {payload.job_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
