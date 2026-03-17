import os
from motor.motor_asyncio import AsyncIOMotorClient
import certifi
from dotenv import load_dotenv
from app.helpers.logging_config import get_logger

load_dotenv()
logger = get_logger("utils.db")

# Database Config
MONGO_DETAILS = os.getenv("MONGO_DETAILS")
DB_NAME = os.getenv("DB_NAME")

# Global client for connection pooling
_client = None


async def get_db():
    """Get database instance with connection pooling."""
    global _client
    try:
        if _client is None:
            logger.info(f"Initializing NEW MongoDB client for: {DB_NAME}")
            _client = AsyncIOMotorClient(
                MONGO_DETAILS, tlsCAFile=certifi.where(), maxPoolSize=50, minPoolSize=10
            )
        else:
            logger.debug("Reusing existing MongoDB client")
        return _client[DB_NAME]
    except Exception as e:
        logger.error(f"DB Connection Error: {e}")
        return None


async def close_db():
    """Close MongoDB client connection pool."""
    global _client
    if _client:
        logger.info("Closing MongoDB connection pool")
        _client.close()
        _client = None
