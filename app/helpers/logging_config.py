# helpers/logging_config.py
import os
import logging
import logging.config

def setup_logging():
    """Configure logging for the application."""
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    log_file = os.getenv("LOG_FILE", "ai-agent.log")
    
    logging_config = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "detailed": {
                "format": "%(asctime)s - %(name)s - %(levelname)s - %(module)s:%(lineno)d - %(message)s"
            },
            "simple": {
                "format": "%(levelname)s - %(message)s"
            }
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "level": log_level,
                "formatter": "simple",
                "stream": "ext://sys.stdout"
            },
            "file": {
                "class": "logging.handlers.RotatingFileHandler",
                "level": "DEBUG",
                "formatter": "detailed",
                "filename": log_file,
                "maxBytes": 10485760,  # 10MB
                "backupCount": 5
            }
        },
        "loggers": {
            "app": {
                "level": log_level,
                "handlers": ["console", "file"],
                "propagate": False
            },
            "app.routers": {
                "level": log_level,
                "handlers": ["console", "file"],
                "propagate": False
            },
            "app.services": {
                "level": log_level,
                "handlers": ["console", "file"],
                "propagate": False
            },
            "app.utils": {
                "level": log_level,
                "handlers": ["console", "file"],
                "propagate": False
            },
            "app.helpers": {
                "level": log_level,
                "handlers": ["console", "file"],
                "propagate": False
            },
            "uvicorn": {
                "level": "INFO",
                "handlers": ["console"],
                "propagate": False
            },
            "fastapi": {
                "level": "INFO", 
                "handlers": ["console"],
                "propagate": False
            },
            "sqlalchemy": {
                "level": "WARNING",
                "handlers": ["file"],
                "propagate": False
            },
            "httpx": {
                "level": "WARNING",
                "handlers": ["file"],
                "propagate": False
            },
            "motor": {
                "level": "WARNING",
                "handlers": ["file"],
                "propagate": False
            }
        },
        "root": {
            "level": log_level,
            "handlers": ["console", "file"]
        }
    }
    
    logging.config.dictConfig(logging_config)
    
    # Force UTF-8 encoding for console handler to support Unicode characters on Windows
    console_handler = logging.getLogger().handlers[0]
    if hasattr(console_handler, 'stream'):
        console_handler.stream.reconfigure(encoding='utf-8')
    
    # Log startup information
    logger = logging.getLogger("app.startup")
    logger.info("Application starting up")
    logger.info(f"Log level set to: {log_level}")
    logger.info(f"Log file: {log_file}")

def get_logger(name: str) -> logging.Logger:
    """Get a logger instance."""
    return logging.getLogger(f"app.{name}")