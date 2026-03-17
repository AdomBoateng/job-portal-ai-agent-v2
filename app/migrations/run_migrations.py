from pymongo import ASCENDING, IndexModel
from motor.motor_asyncio import AsyncIOMotorDatabase
from app.helpers.logging_config import get_logger

logger = get_logger("migrations")

# ---------------------------------------------------------------------------
# Schema definition
# Each key is the collection name; the value is the list of IndexModels to
# ensure after the collection is created.
# ---------------------------------------------------------------------------
COLLECTIONS: dict[str, list[IndexModel]] = {
    "jobs": [
        # job_id is the natural business key used for upserts and look-ups
        IndexModel([("job_id", ASCENDING)], unique=True, name="job_id_unique"),
        IndexModel([("session_id", ASCENDING)], name="jobs_session_id_idx"),
    ],
    "cvs": [
        IndexModel([("application_id", ASCENDING)], name="cvs_application_id_idx"),
        IndexModel([("job_id", ASCENDING)], name="cvs_job_id_idx"),
        IndexModel([("session_id", ASCENDING)], name="cvs_session_id_idx"),
    ],
    "match_reports": [
        IndexModel(
            [("match_report_id", ASCENDING)],
            unique=True,
            name="match_reports_report_id_unique",
        ),
        IndexModel([("job_id", ASCENDING)], name="match_reports_job_id_idx"),
        IndexModel(
            [("application_id", ASCENDING)],
            name="match_reports_application_id_idx",
        ),
    ],
}


async def run_migrations(db: AsyncIOMotorDatabase) -> None:
    """Ensure all required collections and indexes exist.

    Safe to call on every startup — existing collections and indexes are
    left untouched; only missing ones are created.
    """
    logger.info("Running DB migrations...")

    existing = set(await db.list_collection_names())

    for collection_name, indexes in COLLECTIONS.items():
        # Create the collection explicitly if it doesn't exist yet.
        if collection_name not in existing:
            await db.create_collection(collection_name)
            logger.info("Created collection: %s", collection_name)
        else:
            logger.debug("Collection already exists: %s", collection_name)

        # create_indexes is idempotent for indexes with the same name/key.
        if indexes:
            collection = db[collection_name]
            created = await collection.create_indexes(indexes)
            logger.info("Indexes ensured on '%s': %s", collection_name, created)

    logger.info("DB migrations complete.")
