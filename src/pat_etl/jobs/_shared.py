from ..db.mysql import connect

def get_conn(cfg: dict):
    return connect(cfg["mysql"])

def get_dataset_cfg(cfg: dict):
    return cfg["dataset"]

def get_run_cfg(cfg: dict):
    return cfg.get("run", {})
