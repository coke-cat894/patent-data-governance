import os
import yaml
import re
from typing import Any, Dict

_ENV_PATTERN = re.compile(r"^\$\{([A-Z0-9_]+)\}$")

def _expand_env(value: Any) -> Any:
    if isinstance(value, str):
        m = _ENV_PATTERN.match(value.strip())
        if m:
            return os.environ.get(m.group(1), "")
    return value

def _walk(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {k: _walk(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_walk(v) for v in obj]
    return _expand_env(obj)

def load_config(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    cfg = _walk(cfg)

    required = [
        ("mysql", "host"),
        ("mysql", "port"),
        ("mysql", "user"),
        ("mysql", "password"),
        ("mysql", "database"),
        ("dataset", "batch_id"),
        ("dataset", "ods_table"),
        ("dataset", "stage_table"),
    ]
    for a, b in required:
        if not cfg.get(a, {}).get(b):
            raise ValueError(f"Missing config: {a}.{b}")

    cfg.setdefault("run", {})
    cfg["run"].setdefault("max_retries", 5)
    cfg["run"].setdefault("sleep_seconds", 3)
    return cfg
