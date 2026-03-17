import os
import json
import re
import logging
from openai import AsyncOpenAI
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL")
MODEL_NAME = os.getenv("MODEL_NAME", "qwen3:8b")

client = AsyncOpenAI(base_url=OLLAMA_BASE_URL, api_key="ollama")

# Synchronous client for Celery worker context
from openai import OpenAI

sync_client = OpenAI(base_url=OLLAMA_BASE_URL, api_key="ollama")


def _safe_parse_json(raw_content: str) -> dict:
    """
    Extracts the first JSON object from a string, handling markdown fences
    and leading/trailing text common in LLM outputs.
    """
    # Remove common LLM 'noise' like markdown code blocks
    cleaned = re.sub(r"```json|```", "", raw_content).strip()

    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        # Fallback: search for the first '{' and last '}'
        match = re.search(r"(\{.*\})", cleaned, re.DOTALL)
        if match:
            try:
                return json.loads(match.group(1))
            except json.JSONDecodeError:
                pass
    raise ValueError("Failed to extract a valid JSON object from model response.")


def _enforce_business_rules(data: dict) -> dict:
    """
    Instead of failing on minor inconsistencies, this function auto-corrects
    the category based on the numerical score to ensure valid data flow.
    """
    # 1. Ensure score is an integer
    raw_score = data.get("score", 1)
    try:
        score = int(re.search(r"[1-5]", str(raw_score)).group())
    except (AttributeError, ValueError):
        score = 1

    # 2. Normalize Score boundaries
    score = max(1, min(score, 5))

    # 3. Force Category alignment (Business Logic)
    if score >= 4:
        category = "ACCEPT"
    elif score == 3:
        category = "NEED_FURTHER_EVALUATION"
    else:
        category = "REJECT"

    return {
        "score": score,
        "category": category,
        "reason": data.get("reason", "No reason provided by model."),
    }


async def score_cv_with_llm(
    cv_text: str, job_desc: str, job_resp: list, job_skills: list
) -> dict:
    """
    Scores CV using the OpenAI client pattern.
    Includes robust parsing and business rule enforcement.
    """
    prompt = f"""
    Evaluate the CV against the job requirements. 
    Return ONLY a JSON object with: 
    "score" (1-5), "category" (ACCEPT/NEED_FURTHER_EVALUATION/REJECT), and "reason"(a string explaining the reasoning for the score).

    JOB: {job_desc}
    SKILLS: {job_skills}
    RESPONSIBILITIES: {job_resp}
    CV: {cv_text}
    """.strip()

    try:
        # Use the AsyncOpenAI client
        response = await client.chat.completions.create(
            model=MODEL_NAME,
            messages=[
                {
                    "role": "system",
                    "content": "You are an HR bot that only outputs JSON.",
                },
                {"role": "user", "content": prompt},
            ],
            response_format={"type": "json_object"},
            temperature=0,
        )
        raw_content = response.choices[0].message.content
        parsed_json = _safe_parse_json(raw_content)
        return _enforce_business_rules(parsed_json)
    except Exception as e:
        logger.error(f"Scoring failed: {e}")
        return {"score": 1, "category": "REJECT", "reason": f"Parsing Error: {str(e)}"}


def score_cv_with_llm_sync(
    cv_text: str, job_desc: str, job_resp: list, job_skills: list
) -> dict:
    """
    Synchronous version of score_cv_with_llm for use in Celery workers.
    Uses the sync OpenAI client instead of AsyncOpenAI.
    """
    prompt = f"""
    Evaluate the CV against the job requirements. 
    Return ONLY a JSON object with: 
    "score" (1-5), "category" (ACCEPT/NEED_FURTHER_EVALUATION/REJECT), and "reason"(a string explaining the reasoning for the score).

    JOB: {job_desc}
    SKILLS: {job_skills}
    RESPONSIBILITIES: {job_resp}
    CV: {cv_text}
    """.strip()

    try:
        response = sync_client.chat.completions.create(
            model=MODEL_NAME,
            messages=[
                {
                    "role": "system",
                    "content": "You are an HR bot that only outputs JSON.",
                },
                {"role": "user", "content": prompt},
            ],
            response_format={"type": "json_object"},
            temperature=0,
        )
        raw_content = response.choices[0].message.content
        parsed_json = _safe_parse_json(raw_content)
        return _enforce_business_rules(parsed_json)
    except Exception as e:
        logger.error(f"Sync scoring failed: {e}")
        return {"score": 1, "category": "REJECT", "reason": f"Parsing Error: {str(e)}"}


# import os
# import json
# import re
# from typing import List, Dict, Any
# from openai import AsyncOpenAI
# from dotenv import load_dotenv
# from app.helpers.logging_config import get_logger

# load_dotenv()
# logger = get_logger("utils.ollama")

# # LLM Config (Ollama)
# OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL")
# MODEL_NAME = os.getenv("MODEL_NAME")

# client = AsyncOpenAI(base_url=OLLAMA_BASE_URL, api_key="ollama")


# async def score_cv_with_llm(cv_text: str, job_desc: str, job_resp: List[str], job_skills: List[str]) -> Dict[str, Any]:
#     # 1. Enhanced Prompt with Clear Thresholds
#     prompt = f"""
#         You are a Senior Technical Recruiter. Evaluate the CV against the Job Requirements.
#         Use your internal thought process to identify gaps before assigning a score.

#         [JOB DESCRIPTION]
#         {job_desc}
#         Required Skills: {', '.join(job_skills)}
#         Responsibilities: {', '.join(job_resp)}

#         [CANDIDATE CV]
#         {cv_text}

#         [SCORING RUBRIC]
#         5 - Excellent: Perfect match, exceeds all core skills and seniority.
#         4 - Very Good: Strong alignment, meets all mandatory requirements.
#         3 - Good: Solid match, but missing secondary skills or industry depth.
#         2 - Fair: Partial overlap but missing core competencies.
#         1 - Poor: Little to no relevant experience.

#         [TASK]
#         First, analyze the match internally. Then, output a JSON object.
#         JSON Format:
#         {{
#         "thought_process": "Your internal logic and gap analysis",
#         "score": <integer 1-5>,
#         "category": "<SELECT | NEED_FURTHER_EVALUATION | REJECT>",
#         "strengths": [],
#         "gaps": [],
#         "overall_feedback": ""
#         }}
#         """
#     try:
#         # 2. Optimized Parameters for Qwen3-8B
#         response = await client.chat.completions.create(
#             model=MODEL_NAME,
#             messages=[{"role": "user", "content": prompt}],
#             max_tokens=1500,     # Increased to allow for rationale/thought
#             temperature=0.2,    # Low but allows logical flow
#             top_p=0.9,          # Focuses response on high-probability tokens
#             response_format={"type": "json_object"} # Qwen3 supports JSON mode
#         )

#         content = response.choices[0].message.content.strip()

#         # Extract and Validate
#         json_match = re.search(r'\{[\s\S]*\}', content)
#         if json_match:
#             result = json.loads(json_match.group(0))

#             # Map score to the 1-5 scale requested
#             raw_score = int(result.get("score", 1))
#             score = max(1, min(5, raw_score))

#             # 3. Apply Category Threshold Logic
#             # We enforce this in code to ensure consistency regardless of LLM quirks
#             if score >= 4:
#                 category = "SELECT"
#             elif score == 3:
#                 category = "NEED_FURTHER_EVALUATION"
#             else:
#                 category = "REJECT"

#             return {
#                 "score": score,
#                 "category": category,
#                 "strengths": result.get("strengths", []),
#                 "gaps": result.get("gaps", []),
#                 "overall_feedback": result.get("overall_feedback", ""),
#                 "rationale": result.get("thought_process", "") # Capture the 'why'
#             }

#     except Exception as e:
#         logger.error(f"LLM Error: {e}")
#         return {"score": 1, "category": "REJECT", "overall_feedback": "Error processing."}


# async def score_cv_with_llm(cv_text: str, job_desc: str, job_resp: List[str], job_skills: List[str]) -> Dict[str, Any]:
#     """
#     Score a CV against a JD using LLM.
#     Returns a dictionary with score, category, strengths, gaps, and overall_feedback.
#     """
#     logger.debug(f"Scoring CV with LLM - CV length: {len(cv_text)} chars, Job skills: {len(job_skills)}")

#     prompt = f"""
# You are an expert HR AI Assistant specializing in resume evaluation.

# Job Description:
# {job_desc}

# Job Responsibilities:
# {', '.join(job_resp)}

# Required Skills:
# {', '.join(job_skills)}

# Candidate CV:
# {cv_text}

# Evaluation Criteria:
# - Skill match against required skills
# - Relevant experience and seniority
# - Role and domain alignment

# Scoring Rules:
# 1 = Poor match (major gaps, mostly irrelevant)
# 2 = Fair match (some overlap but significant gaps)
# 3 = Good match (meets basic expectations)
# 4 = Very Good match (strong alignment with minor gaps)
# 5 = Excellent match (highly aligned, ideal candidate)

# Task:
# Evaluate the candidate and return STRICT JSON in the following format:

# {{
#   "score": <integer 1-5>,
#   "category": "<SELECT | NEED_FURTHER_EVALUATION | REJECT>",
#   "strengths": ["short bullet point", "..."],
#   "gaps": ["short bullet point", "..."],
#   "overall_feedback": "2–3 concise sentences explaining the decision"
# }}

# Rules:
# - Score must be an integer from 1 to 5.
# - Category rules:
#   - 4–5 → SELECT
#   - 3 → NEED_FURTHER_EVALUATION
#   - 1–2 → REJECT
# - Be factual and concise.
# - Do NOT include any text outside the JSON.
# """

#     try:
#         logger.info(f"Sending request to LLM model: {MODEL_NAME}")
#         response = await client.chat.completions.create(
#             model=MODEL_NAME,
#             messages=[{"role": "user", "content": prompt}],
#             max_tokens=500,
#             temperature=0.0
#         )
#         content = response.choices[0].message.content.strip()
#         logger.debug(f"LLM response: {content}")

#         # Extract JSON from the response
#         # Handle cases where the model might include markdown code blocks
#         json_match = re.search(r'\{[\s\S]*\}', content)
#         if json_match:
#             json_str = json_match.group(0)
#             result = json.loads(json_str)

#             # Validate and extract all fields
#             score = int(result.get("score", 1))
#             score = max(1, min(5, score))  # Ensure score is within 1-5

#             evaluation = {
#                 "score": score,
#                 "category": result.get("category", "REJECT"),
#                 "strengths": result.get("strengths", []),
#                 "gaps": result.get("gaps", []),
#                 "overall_feedback": result.get("overall_feedback", "")
#             }

#             logger.info(f"Successfully scored CV: {score}/5 - Category: {evaluation['category']}")
#             return evaluation
#         else:
#             logger.warning(f"Could not extract JSON from LLM response: {content}")
#             return {
#                 "score": 1,
#                 "category": "REJECT",
#                 "strengths": [],
#                 "gaps": [],
#                 "overall_feedback": "Failed to extract evaluation details."
#             }

#     except json.JSONDecodeError as je:
#         logger.error(f"JSON Parsing Error: {je}", exc_info=True)
#         return {
#             "score": 1,
#             "category": "REJECT",
#             "strengths": [],
#             "gaps": [],
#             "overall_feedback": "Failed to parse model response."
#         }
#     except Exception as e:
#         logger.error(f"LLM Error: {e}", exc_info=True)
#         return {
#             "score": 1,
#             "category": "REJECT",
#             "strengths": [],
#             "gaps": [],
#             "overall_feedback": "LLM request failed."
#         }
