import logging
from typing import Dict, Any


def get_logger(name: str = "ingestion") -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s %(levelname)s %(message)s",
        )
    return logger


def log_batch_summary(logger: logging.Logger, summary: Dict[str, Any]) -> None:
    # Keep it single-line JSON-ish so CloudWatch is searchable
    logger.info("BATCH_SUMMARY %s", summary)
