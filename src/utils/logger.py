import os
import sys
from functools import partial

from loguru import logger

LOG_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "logs", os.environ.get("LOG_DIR_NAME", ""))
if not os.path.exists(LOG_DIR):
    os.mkdir(LOG_DIR)


def filter_log_level(record, level):
    return record["level"].name == level


LOG_FORMAT = f"[{{time}}] [{{process}}] [{{level}}] [{{file}}] [{{line}}] {{message}}"

logger.configure(
    handlers=[
        {
            "sink": os.path.join(LOG_DIR, "info.log"),
            "level": "INFO",
            "format": LOG_FORMAT,
            "rotation": '00:00',
            "enqueue": True,
            "filter": partial(filter_log_level, level="INFO"),
            "retention": "3 days"
        },
        {
            "sink": os.path.join(LOG_DIR, "warning.log"),
            "level": "WARNING",
            "format": LOG_FORMAT,
            "rotation": '00:00',
            "enqueue": True,
            "filter": partial(filter_log_level, level="WARNING"),
            "retention": "3 days"
        },
        {
            "sink": os.path.join(LOG_DIR, "error.log"),
            "level": "ERROR",
            "format": LOG_FORMAT,
            "rotation": '00:00',
            "enqueue": True,
            "filter": partial(filter_log_level, level="ERROR"),
            "retention": "3 days"
        },
    ],
)


def log_error_pro(error: str, e: Exception):
    e = type(e)(f"[{error}] {e}").with_traceback(sys.exc_info()[2])
    log_exception(e)


log_info = logger.info
log_warning = logger.warning
log_error = logger.error
log_exception = logger.exception
