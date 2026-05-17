# -*- coding: utf-8 -*-
"""
Stage clean job (project version)

约束：仅对当前 batch_id 且 is_valid=1 的数据生效；主键 stage_id
"""

import time
import logging
from dataclasses import dataclass
from typing import Tuple, Optional, List
from functools import lru_cache

import pymysql
from pymysql.cursors import DictCursor
from pymysql.err import OperationalError

logger = logging.getLogger(__name__)


# -------------------------
# Helpers: retry
# -------------------------
RETRYABLE_ERRNOS = {1205, 1213, 2006, 2013}  # lock wait, deadlock, server gone, lost conn


def _retryable(e: Exception) -> bool:
    return isinstance(e, OperationalError) and bool(e.args) and e.args[0] in RETRYABLE_ERRNOS


def _with_retry(fn, *, max_retries: int, sleep_seconds: int):
    last = None
    for attempt in range(1, max_retries + 1):
        try:
            return fn()
        except Exception as e:
            last = e
            if attempt == max_retries or not _retryable(e):
                raise
            logger.warning("[RETRY] attempt=%s err=%s", attempt, e)
            time.sleep(sleep_seconds)
    raise last


# -------------------------
# Config
# -------------------------
@dataclass
class JobCfg:
    host: str
    port: int
    user: str
    password: str
    database: str
    charset: str = "utf8mb4"

    stage_table: str = ""
    batch_id: str = ""
    progress_table: str = "etl_stage_clean_progress"
    job_name: str = "clean_stage_x_v1"

    # 运行参数
    segment: int = 200_000
    max_retries: int = 5
    sleep_seconds: int = 3

    # intci_main_name 清洗参数
    clean_fetch_limit: int = 20_000
    tmp_insert_chunk: int = 5_000


def _build_job_cfg(cfg: dict) -> JobCfg:
    mysql = cfg["mysql"]
    ds = cfg["dataset"]
    run = cfg.get("run", {})
    progress = cfg.get("progress", {})

    table_id = ds.get("table_id", "x")
    rule_version = ds.get("rule_version", "v1")

    c = JobCfg(
        host=mysql["host"],
        port=int(mysql["port"]),
        user=mysql["user"],
        password=mysql["password"],
        database=mysql["database"],
        charset=mysql.get("charset", "utf8mb4"),
        stage_table=ds["stage_table"],
        batch_id=ds["batch_id"],
        progress_table=progress.get("stage_clean_progress_table", "etl_stage_clean_progress"),
        job_name=f"clean_stage_{table_id}_{rule_version}",
        segment=int(run.get("segment", 200_000)),
        max_retries=int(run.get("max_retries", 5)),
        sleep_seconds=int(run.get("sleep_seconds", 3)),
    )

    c.clean_fetch_limit = int(run.get("clean_fetch_limit", c.clean_fetch_limit))
    c.tmp_insert_chunk = int(run.get("tmp_insert_chunk", c.tmp_insert_chunk))
    return c


def _connect(c: JobCfg):
    return pymysql.connect(
        host=c.host,
        port=c.port,
        user=c.user,
        password=c.password,
        database=c.database,
        charset=c.charset,
        cursorclass=DictCursor,
        autocommit=False,
    )


# -------------------------
# Progress table (新结构)
# -------------------------
def ensure_progress_table(conn, c: JobCfg):
    sql = f"""
    CREATE TABLE IF NOT EXISTS {c.progress_table} (
      id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
      job_name VARCHAR(128) NOT NULL,
      batch_id VARCHAR(64) NOT NULL,
      last_stage_id BIGINT NOT NULL DEFAULT 0,
      updated_rows BIGINT NOT NULL DEFAULT 0,
      scanned_rows BIGINT NOT NULL DEFAULT 0,
      updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      UNIQUE KEY uk_job_batch (job_name, batch_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()


def load_checkpoint(conn, c: JobCfg) -> Tuple[int, int, int]:
    sql = f"""
    SELECT last_stage_id, updated_rows, scanned_rows
    FROM {c.progress_table}
    WHERE job_name=%s AND batch_id=%s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (c.job_name, c.batch_id))
        row = cur.fetchone()
    if not row:
        return 0, 0, 0
    return int(row["last_stage_id"]), int(row["updated_rows"]), int(row["scanned_rows"])


def save_checkpoint(conn, c: JobCfg, last_stage_id: int, updated_rows: int, scanned_rows: int):
    sql = f"""
    INSERT INTO {c.progress_table}(job_name, batch_id, last_stage_id, updated_rows, scanned_rows)
    VALUES(%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE
      last_stage_id=VALUES(last_stage_id),
      updated_rows=VALUES(updated_rows),
      scanned_rows=VALUES(scanned_rows)
    """
    with conn.cursor() as cur:
        cur.execute(sql, (c.job_name, c.batch_id, last_stage_id, updated_rows, scanned_rows))
    conn.commit()


# =========================================================
# Step 1: 清洗 intci_main_name（改为旧版本处理方式）
# =========================================================

from functools import lru_cache

@lru_cache(maxsize=200_000)
def deduplicate_by_level_fast(data: str, separator: str = "$$$", level_separator: str = ">") -> str:
    """
    目标：按 Section（首字母，如 A/E/G）分组，只保留该 Section 下层级最深的一条完整路径。
    - '$$$' 拆分
    - depth = '>' 数量 + 1
    - section = 第一段（'>' 前）里 '^' 前的编码，再取首字母（例如 E05B -> E）
    - 每个 section 只保留 depth 最大（depth 相同取更长字符串）
    - 保持 section 首次出现顺序输出
    """
    if not data:
        return ""

    s = str(data).strip()
    if not s:
        return ""

    parts = [p.strip() for p in s.split(separator) if p and str(p).strip()]
    if not parts:
        return ""

    best = {}
    order = []

    for p in parts:
        # 深度：层级越深越大
        depth = p.count(level_separator) + 1

        # section：取首段（第一个 '>' 前），再取 '^' 前编码，取首字母
        head = p.split(level_separator, 1)[0].strip()
        code = head.split("^", 1)[0].strip()
        section = (code[0] if code else "").strip()

        if not section:
            # 兜底：极端脏数据，按整段作为组
            section = head or p

        if section not in best:
            order.append(section)
            best[section] = (depth, len(p), p)
        else:
            d0, l0, _ = best[section]
            if depth > d0 or (depth == d0 and len(p) > l0):
                best[section] = (depth, len(p), p)

    return separator.join(best[k][2] for k in order)


def _clean_one_intci_main_name(raw: str) -> str:
    """按旧版本方式对 intci_main_name 做去重/层级优选。"""
    if raw is None:
        return ""
    s = str(raw).strip()
    if not s:
        return ""
    return deduplicate_by_level_fast(s, separator="$$$", level_separator=">")


def clean_intci_main_name(conn, c: JobCfg, l: int, r: int) -> Tuple[int, int, int]:
    """
    分段处理 stage_id [l,r] 内，满足条件的 intci_main_name：
      - batch_id = 当前批次
      - is_valid = 1
      - intci_main_name 非空
      - 含 '$$$' 或 '>'
    做法：
      1) 选出需要清洗的记录（最多 clean_fetch_limit 条）
      2) 计算新值，写入临时表 tmp_clean_intci
      3) join update 回写
    返回：本段 scanned（范围内总行数）、updated（本段实际更新行数）、
          last_processed_id（本次处理到的最大 stage_id，用于 checkpoint 防漏洗）
    """

    # 统计本段扫描量（范围内 valid 行数）
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT COUNT(*) AS c
            FROM {c.stage_table}
            WHERE batch_id=%s AND is_valid=1 AND stage_id BETWEEN %s AND %s
            """,
            (c.batch_id, l, r),
        )
        scanned = int(cur.fetchone()["c"])

    # 取需要清洗的行（限制数量，避免一次内存爆）
    select_sql = f"""
        SELECT stage_id, intci_main_name
        FROM {c.stage_table}
        WHERE batch_id=%s
          AND is_valid=1
          AND stage_id BETWEEN %s AND %s
          AND intci_main_name IS NOT NULL
          AND intci_main_name <> ''
          AND (LOCATE('$$$', intci_main_name) > 0 OR LOCATE('>', intci_main_name) > 0)
        ORDER BY stage_id
        LIMIT %s
    """

    with conn.cursor() as cur:
        cur.execute(select_sql, (c.batch_id, l, r, c.clean_fetch_limit))
        rows = cur.fetchall()

    if not rows:
        return scanned, 0, r

    last_processed_id = int(rows[-1]["stage_id"])

    # 计算新值
    to_upd: List[Tuple[int, str]] = []
    for row in rows:
        sid = int(row["stage_id"])
        raw = row["intci_main_name"]
        newv = _clean_one_intci_main_name(raw)
        # 只更新发生变化的
        if newv and newv != (raw.strip() if isinstance(raw, str) else str(raw).strip()):
            to_upd.append((sid, newv))

    if not to_upd:
        return scanned, 0, last_processed_id

    def _do_update():
        with conn.cursor() as cur:
            cur.execute("DROP TEMPORARY TABLE IF EXISTS tmp_clean_intci")
            cur.execute(
                """
                CREATE TEMPORARY TABLE tmp_clean_intci (
                  stage_id BIGINT NOT NULL PRIMARY KEY,
                  new_intci_main_name TEXT
                ) ENGINE=InnoDB
                """
            )

            ins_sql = "INSERT INTO tmp_clean_intci(stage_id, new_intci_main_name) VALUES (%s,%s)"
            for i in range(0, len(to_upd), c.tmp_insert_chunk):
                cur.executemany(ins_sql, to_upd[i : i + c.tmp_insert_chunk])

            # 回写更新（限定 batch_id + is_valid=1）
            upd_sql = f"""
                UPDATE {c.stage_table} s
                JOIN tmp_clean_intci t ON s.stage_id = t.stage_id
                SET s.intci_main_name = t.new_intci_main_name
                WHERE s.batch_id=%s AND s.is_valid=1
            """
            cur.execute(upd_sql, (c.batch_id,))
            affected = cur.rowcount

        conn.commit()
        return affected

    updated = _with_retry(_do_update, max_retries=c.max_retries, sleep_seconds=c.sleep_seconds)
    return scanned, int(updated), last_processed_id


# -------------------------
# Step 2: deduplicate by patent_id within batch (keep min stage_id)
# -------------------------

def dedup_mark_dirty_segmented(conn, c: JobCfg, segment: int):
    """
    同批次内 patent_id 重复：保留最小 stage_id，其余置 is_valid=0，并追加 invalid_reason
    """
    STAGE = c.stage_table
    BATCH = c.batch_id
    REASON = "重复patent_id"

    TMP_KEEP = "tmp_keep_patent_id"
    TMP_DUP = "tmp_dup_stage_ids"

    def _init_tmp():
        with conn.cursor() as cur:
            cur.execute(f"""
            CREATE TEMPORARY TABLE IF NOT EXISTS {TMP_KEEP} (
              patent_id VARCHAR(255) COLLATE utf8mb4_unicode_ci NOT NULL,
              keep_stage_id BIGINT NOT NULL,
              PRIMARY KEY (patent_id)
            ) ENGINE=InnoDB
            """)
            cur.execute(f"""
            CREATE TEMPORARY TABLE IF NOT EXISTS {TMP_DUP} (
              stage_id BIGINT NOT NULL,
              PRIMARY KEY(stage_id)
            ) ENGINE=InnoDB
            """)
            cur.execute(f"TRUNCATE TABLE {TMP_KEEP}")
            cur.execute(f"TRUNCATE TABLE {TMP_DUP}")
        conn.commit()

    _with_retry(_init_tmp, max_retries=c.max_retries, sleep_seconds=c.sleep_seconds)

    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT MIN(stage_id) AS mi, MAX(stage_id) AS ma
            FROM {STAGE}
            WHERE batch_id=%s AND is_valid=1
              AND patent_id IS NOT NULL AND TRIM(patent_id) <> ''
            """,
            (BATCH,),
        )
        row = cur.fetchone()

    mi = row["mi"]
    ma = row["ma"]
    if mi is None or ma is None:
        logger.info("[dedup] no rows to process batch=%s", BATCH)
        return

    mi, ma = int(mi), int(ma)
    logger.info("[dedup] range=[%s,%s] seg=%s", mi, ma, segment)

    # 1) 分段构建 keep（patent_id -> 全局最小 stage_id）
    l = mi
    while l <= ma:
        r = min(l + segment - 1, ma)

        def _build_keep_seg():
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    INSERT INTO {TMP_KEEP}(patent_id, keep_stage_id)
                    SELECT patent_id, MIN(stage_id) AS keep_stage_id
                    FROM {STAGE}
                    WHERE batch_id=%s AND is_valid=1
                      AND stage_id BETWEEN %s AND %s
                      AND patent_id IS NOT NULL AND TRIM(patent_id) <> ''
                    GROUP BY patent_id
                    ON DUPLICATE KEY UPDATE
                      keep_stage_id = LEAST(keep_stage_id, VALUES(keep_stage_id))
                    """,
                    (BATCH, l, r),
                )
            conn.commit()

        _with_retry(_build_keep_seg, max_retries=c.max_retries, sleep_seconds=c.sleep_seconds)
        l = r + 1

    # 2) 分段写入 dup stage_id（join keep，排除 keep）
    l = mi
    while l <= ma:
        r = min(l + segment - 1, ma)

        def _build_dup_seg():
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    INSERT IGNORE INTO {TMP_DUP}(stage_id)
                    SELECT s.stage_id
                    FROM {STAGE} s
                    JOIN {TMP_KEEP} k
                      ON k.patent_id = s.patent_id
                    WHERE s.batch_id=%s AND s.is_valid=1
                      AND s.stage_id BETWEEN %s AND %s
                      AND s.stage_id <> k.keep_stage_id
                    """,
                    (BATCH, l, r),
                )
            conn.commit()

        _with_retry(_build_dup_seg, max_retries=c.max_retries, sleep_seconds=c.sleep_seconds)
        l = r + 1

    # 3) 分段 update：join dup 表置脏
    l = mi
    while l <= ma:
        r = min(l + segment - 1, ma)

        def _update_seg():
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE {STAGE} s
                    JOIN {TMP_DUP} d ON d.stage_id = s.stage_id
                    SET s.is_valid = 0,
                        s.invalid_reason = CASE
                          WHEN s.invalid_reason IS NULL OR TRIM(s.invalid_reason)='' THEN %s
                          WHEN LOCATE(%s, s.invalid_reason) > 0 THEN s.invalid_reason
                          ELSE CONCAT(s.invalid_reason, ';', %s)
                        END,
                        s.updated_date = CURRENT_TIMESTAMP
                    WHERE s.batch_id=%s
                      AND s.stage_id BETWEEN %s AND %s
                    """,
                    (REASON, REASON, REASON, BATCH, l, r),
                )
            conn.commit()

        _with_retry(_update_seg, max_retries=c.max_retries, sleep_seconds=c.sleep_seconds)
        l = r + 1

    logger.info("[dedup] done batch=%s", BATCH)


# -------------------------
# Step 3: fix country/province
# -------------------------
def fix_stage_country_province_segmented(conn, c: JobCfg, segment: Optional[int] = None):
    segment = int(segment or c.segment)
    STAGE = c.stage_table  # ✅ 修复：原来你这里用 STAGE 但没定义

    with conn.cursor() as cur:
        cur.execute(
            f"SELECT MIN(stage_id) AS mn, MAX(stage_id) AS mx FROM {STAGE} WHERE batch_id=%s AND is_valid=1",
            (c.batch_id,),
        )
        row = cur.fetchone()
        mn = row["mn"]
        mx = row["mx"]
    if mn is None or mx is None:
        logger.info("[fix_region] no rows for batch=%s", c.batch_id)
        return

    logger.info("[fix_region] batch=%s range=[%s,%s] seg=%s", c.batch_id, mn, mx, segment)

    l = int(mn)
    max_id = int(mx)

    while l <= max_id:
        r = l + segment - 1

        def _do():
            with conn.cursor() as cur:
                # 1) 境外省份：根据 dict_country 回填省份为国家中文名
                sql_fix_foreign = f"""
                UPDATE {STAGE} s
                JOIN dict_country d
                  ON s.appl_country COLLATE utf8mb4_0900_ai_ci
                   = d.country_code COLLATE utf8mb4_0900_ai_ci
                SET s.appl_province = d.country_name_cn
                WHERE s.batch_id=%s
                  AND s.stage_id BETWEEN %s AND %s
                  AND s.is_valid = 1
                  AND s.appl_country <> 'CN'
                  AND (s.appl_province IS NULL OR TRIM(s.appl_province) = '')
                """
                cur.execute(sql_fix_foreign, (c.batch_id, l, r))

                # 2) WO：省份置空
                sql_fix_wo = f"""
                UPDATE {STAGE}
                SET appl_province = NULL
                WHERE batch_id=%s
                  AND stage_id BETWEEN %s AND %s
                  AND is_valid = 1
                  AND appl_country = 'WO'
                """
                cur.execute(sql_fix_wo, (c.batch_id, l, r))

                # 3) HK / TW / MO：国家归 CN
                sql_fix_hktwmo = f"""
                UPDATE {STAGE}
                SET appl_country = 'CN'
                WHERE batch_id=%s
                  AND stage_id BETWEEN %s AND %s
                  AND is_valid = 1
                  AND appl_country IN ('HK','TW','MO')
                """
                cur.execute(sql_fix_hktwmo, (c.batch_id, l, r))

                # 4) signory_item 去 ".0"
                sql_fix_signory_item = f"""
                UPDATE {STAGE} s
                SET s.signory_item = CAST(CAST(TRIM(s.signory_item) AS DECIMAL(30,10)) AS UNSIGNED)
                WHERE s.batch_id=%s
                  AND s.stage_id BETWEEN %s AND %s
                  AND s.is_valid = 1
                  AND s.signory_item IS NOT NULL
                  AND TRIM(s.signory_item) REGEXP '^[0-9]+\\\\.0+$'
                """
                cur.execute(sql_fix_signory_item, (c.batch_id, l, r))

                # 5) application_origin 更新
                sql_application_origin = f"""
                UPDATE {STAGE} s
                SET s.application_origin =
                  CASE
                    WHEN s.appl_country = 'WO' THEN 'PCT国际申请'
                    WHEN s.appl_country = 'CN' THEN '境内专利'
                    WHEN s.appl_country IS NULL OR TRIM(s.appl_country) = '' THEN NULL
                    ELSE '境外专利'
                  END
                WHERE s.batch_id=%s
                  AND s.stage_id BETWEEN %s AND %s
                  AND s.is_valid = 1
                """
                cur.execute(sql_application_origin, (c.batch_id, l, r))

            conn.commit()
            return 1

        _with_retry(_do, max_retries=c.max_retries, sleep_seconds=c.sleep_seconds)
        l = r + 1


# -------------------------
# Entry
# -------------------------
def run_stage_clean(cfg: dict):
    c = _build_job_cfg(cfg)

    logger.info(
        "[stage_clean] stage=%s batch_id=%s segment=%s job=%s progress=%s",
        c.stage_table, c.batch_id, c.segment, c.job_name, c.progress_table
    )

    conn = _connect(c)
    try:
        _with_retry(lambda: ensure_progress_table(conn, c), max_retries=c.max_retries, sleep_seconds=c.sleep_seconds)

        last_stage_id, updated_total, scanned_total = _with_retry(
            lambda: load_checkpoint(conn, c),
            max_retries=c.max_retries,
            sleep_seconds=c.sleep_seconds,
        )

        with conn.cursor() as cur:
            cur.execute(
                f"SELECT MIN(stage_id) AS mn, MAX(stage_id) AS mx FROM {c.stage_table} WHERE batch_id=%s AND is_valid=1",
                (c.batch_id,),
            )
            mm = cur.fetchone()

        if not mm or mm["mn"] is None:
            logger.info("[stage_clean] no valid rows for batch=%s", c.batch_id)
            return

        min_id = int(mm["mn"])
        max_id = int(mm["mx"])

        # 从 checkpoint 继续
        l = max(int(last_stage_id) + 1, min_id)

        # Step1: 清洗 intci_main_name（旧版本逻辑 + 防漏洗）
        while l <= max_id:
            r = min(l + c.segment - 1, max_id)

            def _do_one():
                return clean_intci_main_name(conn, c, l, r)

            scanned, updated, last_processed_id = _with_retry(
                _do_one, max_retries=c.max_retries, sleep_seconds=c.sleep_seconds
            )

            scanned_total += scanned
            updated_total += updated

            _with_retry(
                lambda: save_checkpoint(conn, c, last_processed_id, updated_total, scanned_total),
                max_retries=c.max_retries,
                sleep_seconds=c.sleep_seconds,
            )

            logger.info(
                "[stage_clean] seg=[%s,%s] scanned=%s updated=%s last_processed_id=%s totals(scanned=%s updated=%s)",
                l, r, scanned, updated, last_processed_id, scanned_total, updated_total
            )

            l = last_processed_id + 1

        # Step2: 去重置脏
        # dedup_mark_dirty_segmented(conn, c, segment=c.segment)

        # Step3: 国家省份修复 & 其他字段修复
        # fix_stage_country_province_segmented(conn, c, segment=c.segment)

        logger.info("[stage_clean] done batch=%s", c.batch_id)

    finally:
        conn.close()