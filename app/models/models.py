from pydantic import BaseModel
from typing import List, Optional


class JDPayload(BaseModel):
    title: str
    job_id: str
    description: str
    responsibilities: List[str]
    skills: List[str]
    session_id: Optional[str] = None


class CVPayload(BaseModel):
    session_id: Optional[str] = None
    application_id: str
    filename: str
    resume: str
    job_id: Optional[str] = None
    middleware_callback_url: Optional[str] = None  # Where to send results


class JDResponse(BaseModel):
    job_id: str
    title: str
    message: str


class CVResponse(BaseModel):
    application_id: str
    filename: str
    message: str


class ResultPayload(BaseModel):
    session_id: str
    application_id: str
    job_id: str
    score: int
    rationale: str
    match_report_id: str
    created_at: str


class ScoreRequestPayload(BaseModel):
    """Single request containing BOTH the Job + CV for one middleware session."""

    session_id: str

    # Job snapshot (must be the job tied to THIS session/application)
    job_id: str
    title: str
    description: str
    responsibilities: List[str] = []
    skills: List[str] = []

    # Application/CV
    application_id: str
    filename: str
    resume: str

    # Callback
    middleware_callback_url: str


class ScoreResponse(BaseModel):
    session_id: str
    job_id: str
    application_id: str
    message: str
