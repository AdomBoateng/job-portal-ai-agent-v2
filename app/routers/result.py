from fastapi import APIRouter, HTTPException
from app.utils.db import get_db
from fastapi.responses import FileResponse
import pandas as pd
from app.helpers.logging_config import get_logger

logger = get_logger("routers.result")

router = APIRouter()

@router.get("/report")
async def generate_report():
    try:
        logger.info("Generating match reports Excel file")
        db = await get_db()
        if db is None:
            logger.error("Database unavailable when generating report")
            raise HTTPException(status_code=500, detail="Database Unavailable")
        
        # Fetch all Match Reports
        cursor = db["match_reports"].find({})
        reports = await cursor.to_list(length=1000)
        logger.info(f"Found {len(reports)} match reports")
        
        # Fetch all JDs and CVs for lookup
        jds = await db["jobs"].find({}).to_list(length=1000)
        cvs = await db["cvs"].find({}).to_list(length=1000)
        logger.debug(f"Fetched {len(jds)} jobs and {len(cvs)} CVs for lookup")
        
        jd_map = {str(jd.get("job_id")): jd.get("title", "Unknown Role") for jd in jds}
        cv_map = {}
        for cv in sorted(
            cvs,
            key=lambda item: (item.get("created_at") is not None, item.get("created_at")),
        ):
            cv_map[str(cv.get("application_id"))] = cv.get("filename", "Unknown Candidate")
        
        data = []
        for r in reports:
            job_id = r.get("job_id")
            application_id = r.get("application_id")
            data.append({
                "JD ID": str(job_id),
                "Role": jd_map.get(str(job_id), "Unknown"),
                "CV ID": str(application_id),
                "Candidate": cv_map.get(str(application_id), "Unknown"),
                "Score": r.get("score", 0),
                "Report ID": r.get("match_report_id"),
                "Date": r.get("created_at")
            })
            
        if not data:
            logger.warning("No reports found to generate Excel file")
            return {"message": "No reports found"}
            
        df = pd.DataFrame(data)
        filename = "cv_scores_report.xlsx"
        df.to_excel(filename, index=False)
        logger.info(f"Successfully generated report: {filename} with {len(data)} entries")
        
        return FileResponse(filename, filename=filename, media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    except Exception as e:
        import traceback
        logger.error(f"Error generating report: {e}", exc_info=True)
        traceback.print_exc()
        return {"error": str(e), "trace": traceback.format_exc()}


@router.get("/results/{job_id}")
async def get_results_for_job(job_id: str):
    """
    Get all screening results for a specific JD.
    Returns list of CVs with their scores.
    """
    try:
        logger.info(f"Fetching results for job: {job_id}")
        db = await get_db()
        if db is None:
            logger.error("Database unavailable when fetching results")
            raise HTTPException(status_code=500, detail="Database Unavailable")
        
        # Fetch Job
        job = await db["jobs"].find_one({"job_id": job_id})
        if not job:
            logger.warning(f"Job not found: {job_id}")
            raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
        
        # Fetch all match reports for this Job
        reports = await db["match_reports"].find({"job_id": job_id}).to_list(length=None)
        logger.info(f"Found {len(reports)} results for job {job_id}")
        
        results = []
        for report in reports:
            cv_cursor = db["cvs"].find({"application_id": report.get("application_id")}).sort(
                [("created_at", -1), ("_id", -1)]
            )
            cv_candidates = await cv_cursor.to_list(length=1)
            cv = cv_candidates[0] if cv_candidates else None
            if cv:
                results.append({
                    "application_id": report.get("application_id"),
                    "filename": cv.get("filename"),
                    "score": report.get("score"),
                    "report_sent": cv.get("report_sent", False),
                    "created_at": report.get("created_at")
                })
        
        logger.debug(f"Prepared {len(results)} results for job {job_id}")
        return {
            "job_id": job_id,
            "job_title": job.get("title"),
            "total_results": len(results),
            "results": sorted(results, key=lambda x: x["score"], reverse=True)
        }
    except Exception as e:
        logger.error(f"Error fetching results for job {job_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
