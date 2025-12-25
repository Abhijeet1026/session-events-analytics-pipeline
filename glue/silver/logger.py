# glue/silver/logger.py

import logging
import sys
from typing import Optional


def get_logger(name: str = "silver", level: Optional[str] = None) -> logging.Logger:
    """
    Glue/Spark-friendly logger:
    - logs to stdout (CloudWatch captures it)
    - avoids duplicate handlers on repeated imports
    - consistent format across modules
    """
    logger = logging.getLogger(name)

    if logger.handlers:
        return logger  # already configured

    lvl = (level or "INFO").upper()
    logger.setLevel(getattr(logging, lvl, logging.INFO))
    logger.propagate = False

    handler = logging.StreamHandler(sys.stdout)
    fmt = "%(asctime)s %(levelname)s %(name)s - %(message)s"
    handler.setFormatter(logging.Formatter(fmt))
    handler.setLevel(logger.level)

    logger.addHandler(handler)
    return logger
