import os
import logging
from datetime import datetime

import certifi
import requests
from bson import ObjectId
from pymongo import MongoClient
from dotenv import load_dotenv

from app.celery_app import celery_app
from app.utils.agent import score_cv_with_llm_sync

load_dotenv()

logger = logging.getLogger("app.tasks")

# ── Synchronous MongoDB helper ──────────────────────────────────────────────

MONGO_DETAILS = os.getenv("MONGO_DETAILS")
DB_NAME = os.getenv("DB_NAME")

_sync_client = None


def get_sync_db():
    """Get a synchronous pymongo database handle (cached per-worker)."""
    global _sync_client
    if _sync_client is None:
        _sync_client = MongoClient(
            MONGO_DETAILS,
            tlsCAFile=certifi.where(),
            maxPoolSize=20,
            minPoolSize=2,
        )
        logger.info("Initialized synchronous MongoDB client for Celery worker")
    return _sync_client[DB_NAME]


# ── Cache for Job metadata (per-worker) ─────────────────────────────────────

_job_cache = {}  # key: (session_id, job_id)


# ── Celery Tasks ─────────────────────────────────────────────────────────────


@celery_app.task(
    bind=True,
    name="app.tasks.score_single_cv_task",
    max_retries=3,
    default_retry_delay=10,
    acks_late=True,
)
def score_single_cv_task(
    self,
    application_id: str = None,
    middleware_callback_url: str = None,
    cv_id: str = None,
    session_id: str = None,
):
    """
    Score a single CV against its JD and POST the result to middleware.

    This is the Celery equivalent of the old `process_single_cv` background task.
    Concurrency is controlled by Celery's worker_concurrency setting.
    """
    logger.info(
        "Task %s: scoring CV application_id=%s cv_id=%s",
        self.request.id,
        application_id,
        cv_id,
    )

    db = get_sync_db()

    try:
        if not application_id and not cv_id:
            logger.error("score_single_cv_task called without application_id or cv_id")
            return {"status": "error", "detail": "No application_id or cv_id provided"}

        # ── Fetch CV ────────────────────────────────────────────────────
        cv = None
        if cv_id:
            try:
                cv = db["cvs"].find_one({"_id": ObjectId(cv_id)})
            except Exception:
                logger.warning("Invalid cv_id supplied: %s", cv_id)

        if cv is None and application_id:
            cv_filter = {"application_id": application_id}
            if session_id:
                cv_filter["session_id"] = session_id
            cv = db["cvs"].find_one(
                cv_filter,
                sort=[("processed", 1), ("created_at", -1), ("_id", -1)],
            )

        if not cv:
            msg = f"CV not found: application_id={application_id} cv_id={cv_id}"
            logger.error(msg)
            return {"status": "error", "detail": msg}

        selected_application_id = cv.get("application_id")
        logger.info(
            "Selected CV _id=%s application_id=%s",
            cv.get("_id"),
            selected_application_id,
        )

        job_id = cv.get("job_id")
        if not job_id:
            logger.warning("CV %s has no linked job, skipping", selected_application_id)
            return {"status": "skipped", "detail": "No job_id on CV"}

        # ── Determine session_id ────────────────────────────────────────
        effective_session_id = session_id or cv.get("session_id")
        if not effective_session_id:
            logger.warning(
                "CV %s has no session_id, cannot select JD", selected_application_id
            )
            return {"status": "skipped", "detail": "No session_id"}

        cache_key = (effective_session_id, job_id)

        # ── Fetch JD (with cache) ──────────────────────────────────────
        job = _job_cache.get(cache_key)
        if not job:
            job = db["jobs"].find_one(
                {"job_id": job_id, "session_id": effective_session_id}
            )
            if job:
                _job_cache[cache_key] = job
                logger.debug(
                    "Cached job snapshot job_id=%s session=%s",
                    job_id,
                    effective_session_id,
                )
            else:
                msg = f"Job snapshot not found: job_id={job_id} session={effective_session_id}"
                logger.error(msg)
                return {"status": "error", "detail": msg}

        session_id_final = effective_session_id

        # ── Prepare data ────────────────────────────────────────────────
        cv_text = cv.get("extracted_text", "")
        job_desc = job.get("description", "")
        job_resp = job.get("responsibilities", [])
        job_skills = job.get("skills", [])

        # ── Score with LLM (synchronous) ────────────────────────────────
        logger.info("Scoring CV %s for Job %s", selected_application_id, job_id)
        evaluation = score_cv_with_llm_sync(cv_text, job_desc, job_resp, job_skills)

        score = int(evaluation["score"])
        rationale = evaluation["reason"]
        category = evaluation.get("category")
        logger.info(
            "[OK] Scored CV %s: %d/5, category=%s",
            selected_application_id,
            score,
            category,
        )

        # ── Create Match Report ─────────────────────────────────────────
        match_report = {
            "session_id": session_id_final,
            "match_report_id": f"report_{selected_application_id}_{job_id}_{int(datetime.now().timestamp())}",
            "application_id": selected_application_id,
            "job_id": job_id,
            "score": score,
            "rationale": rationale,
            "created_at": datetime.now(),
        }
        db["match_reports"].insert_one(match_report)
        logger.debug("Created match report: %s", match_report["match_report_id"])

        # ── Prepare result payload ──────────────────────────────────────
        result_payload = {
            "session_id": session_id_final,
            "job_id": job_id,
            "application_id": selected_application_id,
            "score": score,
            "rationale": rationale,
            "match_report_id": match_report["match_report_id"],
            "created_at": match_report["created_at"].isoformat(),
        }

        # ── Send result to middleware ───────────────────────────────────
        report_sent = False
        callback_url = middleware_callback_url or cv.get("middleware_callback_url")
        if callback_url:
            logger.info("Sending result to middleware: %s", callback_url)
            try:
                resp = requests.post(
                    callback_url,
                    json=result_payload,
                    headers={"Content-Type": "application/json"},
                    timeout=10,
                )
                if resp.status_code in (200, 201, 202):
                    report_sent = True
                    logger.info(
                        "[OK] Result sent to middleware for CV %s",
                        selected_application_id,
                    )
                else:
                    logger.warning(
                        "[WARN] Middleware returned %d for CV %s",
                        resp.status_code,
                        selected_application_id,
                    )
            except Exception as e:
                logger.error("[WARN] Failed to send result to middleware: %s", e)

        # ── Mark CV as processed ────────────────────────────────────────
        update_data = {
            "processed": True,
            "score": score,
            "report_sent": report_sent,
            "report_sent_at": datetime.now() if report_sent else None,
        }
        db["cvs"].update_one({"_id": cv["_id"]}, {"$set": update_data})
        logger.info(
            "[OK] CV %s marked as processed (report_sent=%s)",
            selected_application_id,
            report_sent,
        )

        return {
            "status": "success",
            "application_id": selected_application_id,
            "score": score,
            "category": category,
            "report_sent": report_sent,
        }

    except Exception as exc:
        logger.error(
            "[ERROR] Error processing CV %s / cv_id %s: %s",
            application_id,
            cv_id,
            exc,
            exc_info=True,
        )
        # Retry on transient failures (network, LLM timeouts, etc.)
        raise self.retry(exc=exc)


@celery_app.task(
    name="app.tasks.batch_score_cvs_task",
    acks_late=True,
)
def batch_score_cvs_task():
    """
    Process all unprocessed CVs in batch mode by dispatching individual
    scoring tasks. Replaces the old `process_cvs_background`.
    """
    logger.info("Starting batch CV scoring via Celery")
    db = get_sync_db()

    cvs = list(
        db["cvs"]
        .find({"processed": False, "job_id": {"$exists": True, "$ne": None}})
        .limit(100)
    )

    if not cvs:
        logger.info("No unprocessed CVs found")
        return {"status": "complete", "dispatched": 0}

    logger.info("Found %d unprocessed CVs, dispatching tasks", len(cvs))

    dispatched = 0
    for cv in cvs:
        if cv.get("application_id"):
            score_single_cv_task.delay(
                application_id=cv.get("application_id"),
                cv_id=str(cv.get("_id")),
                session_id=cv.get("session_id"),
            )
            dispatched += 1

    logger.info("Dispatched %d scoring tasks", dispatched)
    return {"status": "complete", "dispatched": dispatched}
