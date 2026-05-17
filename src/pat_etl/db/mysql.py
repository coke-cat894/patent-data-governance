import time
import pymysql
from pymysql.err import OperationalError
from typing import Dict, Any, Callable

RETRYABLE_ERRNOS = {1205, 1213, 2006, 2013}

def connect(mysql_cfg: Dict[str, Any]):
    cfg = dict(mysql_cfg)
    cfg.setdefault("autocommit", False)
    return pymysql.connect(**cfg)

def is_retryable(e: Exception) -> bool:
    if isinstance(e, OperationalError) and e.args:
        errno = e.args[0]
        return errno in RETRYABLE_ERRNOS
    return False

def with_retry(fn: Callable, *, max_retries: int = 5, sleep_seconds: int = 3):
    last = None
    for attempt in range(1, max_retries + 1):
        try:
            return fn()
        except Exception as e:
            last = e
            if attempt == max_retries or not is_retryable(e):
                raise
            time.sleep(sleep_seconds)
    raise last
