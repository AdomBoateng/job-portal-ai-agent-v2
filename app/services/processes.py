"""
Legacy process functions.

These functions are kept for backwards compatibility and direct invocation,
but the primary execution path now goes through Celery tasks in app.tasks.

The async versions are retained for potential direct async usage within
the FastAPI process (e.g., for testing or lightweight operations).
"""

import asyncio
from typing import Optional
from datetime import datetime
import httpx
from bson import ObjectId

from app.utils.db import get_db
from app.utils.agent import score_cv_with_llm
from app.models.models import ResultPayload
from app.helpers.logging_config import get_logger

logger = get_logger("services.processes")

# Cache for Job metadata to avoid repeated DB lookups
job_cache = {}  # key: (session_id, job_id)


async def process_cvs_background():
    """Process all unprocessed CVs in batch mode concurrently."""
    logger.info("Starting concurrent batch CV processing (async)")
    db = await get_db()
    if db is None:
        logger.error("Database unavailable for batch processing")
        return

    # 1. Fetch unprocessed CVs
    cv_cursor = db["cvs"].find(
        {"processed": False, "job_id": {"$exists": True, "$ne": None}}
    )
    cvs = await cv_cursor.to_list(length=100)

    if not cvs:
        logger.info("No unprocessed CVs found")
        return

    logger.info(f"Found {len(cvs)} unprocessed CVs to process")

    # Process CVs concurrently
    tasks = [
        process_single_cv(
            cv.get("application_id"),
            cv_id=str(cv.get("_id")),
            session_id=cv.get("session_id"),
        )
        for cv in cvs
        if cv.get("application_id")
    ]
    await asyncio.gather(*tasks)
    logger.info("Finished batch processing")


async def process_single_cv(
    application_id: Optional[str],
    middleware_callback_url: Optional[str] = None,
    cv_id: Optional[str] = None,
    session_id: Optional[str] = None,
):
    """
    Process a single CV: Score it against its JD and send results back to middleware.
    Only marks CV as processed AFTER successfully sending results.

    NOTE: This async version is retained for backwards compatibility.
    The primary path now uses Celery tasks (see app.tasks.score_single_cv_task).
    """
    logger.info(
        f"Starting async processing for CV: application_id={application_id}, cv_id={cv_id}"
    )
    db = await get_db()
    if db is None:
        logger.error(f"Database unavailable for CV {application_id}")
        return

    try:
        if not application_id and not cv_id:
            logger.error("process_single_cv called without application_id or cv_id")
            return

        # Fetch the exact CV record if cv_id is provided; otherwise pick latest unprocessed/newest for application_id
        cv = None
        if cv_id:
            try:
                cv = await db["cvs"].find_one({"_id": ObjectId(cv_id)})
            except Exception:
                logger.warning(f"Invalid cv_id supplied for processing: {cv_id}")

        if cv is None and application_id:
            cv_filter = {"application_id": application_id}
            if session_id:
                cv_filter["session_id"] = session_id
            cv_cursor = (
                db["cvs"]
                .find(cv_filter)
                .sort([("processed", 1), ("created_at", -1), ("_id", -1)])
            )
            cv_candidates = await cv_cursor.to_list(length=1)
            cv = cv_candidates[0] if cv_candidates else None

        if not cv:
            logger.error(
                f"CV not found for application_id={application_id}, cv_id={cv_id}"
            )
            return

        selected_application_id = cv.get("application_id")
        logger.info(
            f"Selected CV document _id={cv.get('_id')} application_id={selected_application_id}"
        )

        job_id = cv.get("job_id")
        if not job_id:
            logger.warning(f"CV {selected_application_id} has no linked Job, skipping")
            return

        effective_session_id = session_id or cv.get("session_id")
        if not effective_session_id:
            logger.warning(
                f"CV {selected_application_id} has no session_id, cannot safely select the correct JD"
            )
            return

        cache_key = (effective_session_id, job_id)

        # Fetch the JD snapshot for THIS session (check cache first)
        job = job_cache.get(cache_key)
        if not job:
            job = await db["jobs"].find_one(
                {"job_id": job_id, "session_id": effective_session_id}
            )
            if job:
                job_cache[cache_key] = job
            else:
                logger.error(
                    f"Job snapshot not found for job_id={job_id} session_id={effective_session_id} (CV {selected_application_id})"
                )
                return

        session_id = effective_session_id

        cv_text = cv.get("extracted_text", "")
        job_desc = job.get("description", "")
        job_resp = job.get("responsibilities", [])
        job_skills = job.get("skills", [])

        # Score with LLM
        evaluation = await score_cv_with_llm(cv_text, job_desc, job_resp, job_skills)

        score = int(evaluation["score"])
        rationale = evaluation["reason"]
        category = evaluation.get("category")
        logger.info(
            f"[OK] Scored CV {selected_application_id} for Job {job_id}: {score}/5, category: {category}"
        )

        # Create Match Report
        match_report = {
            "session_id": session_id,
            "match_report_id": f"report_{selected_application_id}_{job_id}_{int(datetime.now().timestamp())}",
            "application_id": selected_application_id,
            "job_id": job_id,
            "score": score,
            "rationale": rationale,
            "created_at": datetime.now(),
        }
        await db["match_reports"].insert_one(match_report)

        # Prepare result
        result = ResultPayload(
            session_id=session_id,
            job_id=job_id,
            application_id=selected_application_id,
            score=score,
            rationale=rationale,
            match_report_id=match_report["match_report_id"],
            created_at=match_report["created_at"].isoformat(),
        )

        # Send result back to middleware if callback URL provided
        report_sent = False
        callback_url = middleware_callback_url or cv.get("middleware_callback_url")
        if callback_url:
            try:
                async with httpx.AsyncClient(timeout=10.0) as client:
                    response = await client.post(
                        callback_url,
                        json=result.dict(),
                        headers={"Content-Type": "application/json"},
                    )
                if response.status_code in [200, 201, 202]:
                    report_sent = True
                    logger.info(
                        f"[OK] Result sent to middleware for CV {selected_application_id}"
                    )
                else:
                    logger.warning(
                        f"[WARN] Middleware returned {response.status_code} for CV {selected_application_id}"
                    )
            except Exception as e:
                logger.error(f"[WARN] Failed to send result to middleware: {e}")

        # ONLY mark as processed AFTER successfully sending results (or if no callback)
        update_data = {
            "processed": True,
            "score": score,
            "report_sent": report_sent,
            "report_sent_at": datetime.now() if report_sent else None,
        }

        await db["cvs"].update_one({"_id": cv["_id"]}, {"$set": update_data})
        logger.info(
            f"[OK] CV {selected_application_id} marked as processed (report_sent={report_sent})"
        )

    except Exception as e:
        logger.error(
            f"[ERROR] Error processing CV {application_id} / cv_id {cv_id}: {e}",
            exc_info=True,
        )
