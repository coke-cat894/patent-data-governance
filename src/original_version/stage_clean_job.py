import time
from dataclasses import dataclass
from functools import lru_cache
from typing import Tuple

import pymysql
from pymysql.err import OperationalError
from pymysql.cursors import DictCursor


# =========================
# 1) 配置区（集中管理）
# =========================
@dataclass(frozen=True)
class Config:
    # DB
    host: str = "172.16.0.60"
    port: int = 13306
    user: str = "root"
    password: str = "KpsMysql666"
    database: str = "mdc_pat"
    charset: str = "utf8mb4"
    autocommit: bool = False
    DictCursor: str = 'DictCursor'

    # 清洗目标表和批次号
    stage_table: str = "dwd_patents_stage_g"
    batch_id: str = "batch_20260108_153537"

    # intci_main_name clean去重清洗
    clean_batch_size: int = 20000
    job_name: str = "clean_stage_g_v1"
    progress_table: str = "etl_stage_clean_progress_old"

    # dedup dirty mark
    dedup_segment: int = 200_000


# =========================
# 2) 通用工具
# =========================
def connect_db(cfg: Config):
    return pymysql.connect(
        host=cfg.host,
        port=cfg.port,
        user=cfg.user,
        password=cfg.password,
        database=cfg.database,
        charset=cfg.charset,
        autocommit=cfg.autocommit,
        cursorclass=DictCursor,
    )


def is_retryable(e: Exception) -> bool:
    msg = str(e)
    # lock wait timeout / deadlock / gone away / lost connection / JDBC EOF
    return any(code in msg for code in ["1205", "1213", "2006", "2013", "08S01"])


def run_with_retries(fn, max_retries=5, sleep_sec=10):
    for attempt in range(1, max_retries + 1):
        try:
            return fn()
        except OperationalError as e:
            if attempt == max_retries or not is_retryable(e):
                raise
            print(f"[RETRY] attempt={attempt}/{max_retries} err={e}")
            time.sleep(sleep_sec)


# =========================
# 3) 进度表（断点续跑）
# =========================
def ensure_progress_table(conn, cfg: Config):
    sql = f"""
    CREATE TABLE IF NOT EXISTS {cfg.progress_table} (
      stage_table   VARCHAR(128) NOT NULL,
      batch_id      VARCHAR(64)  NOT NULL,
      job_name      VARCHAR(64)  NOT NULL,
      last_stage_id BIGINT       NOT NULL DEFAULT 0,
      updated_rows  BIGINT       NOT NULL DEFAULT 0,
      scanned_rows  BIGINT       NOT NULL DEFAULT 0,
      updated_date  TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP
                    ON UPDATE CURRENT_TIMESTAMP,
      PRIMARY KEY (stage_table, batch_id, job_name)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()


def load_checkpoint(conn, cfg: Config) -> Tuple[int, int, int]:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT last_stage_id, updated_rows, scanned_rows
            FROM {cfg.progress_table}
            WHERE stage_table=%s AND batch_id=%s AND job_name=%s
            """,
            (cfg.stage_table, cfg.batch_id, cfg.job_name),
        )
        row = cur.fetchone()
    if not row:
        return 0, 0, 0
    return int(row[0]), int(row[1]), int(row[2])


def save_checkpoint(conn, cfg: Config, last_stage_id: int, updated_rows: int, scanned_rows: int):
    with conn.cursor() as cur:
        cur.execute(
            f"""
            INSERT INTO {cfg.progress_table}
              (stage_table, batch_id, job_name, last_stage_id, updated_rows, scanned_rows)
            VALUES (%s,%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE
              last_stage_id=VALUES(last_stage_id),
              updated_rows=VALUES(updated_rows),
              scanned_rows=VALUES(scanned_rows),
              updated_date=CURRENT_TIMESTAMP
            """,
            (cfg.stage_table, cfg.batch_id, cfg.job_name, last_stage_id, updated_rows, scanned_rows),
        )
    conn.commit()


# =========================
# 4) intci_main_name 清洗（高性能）
# =========================
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


def ensure_tmp_clean_table(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TEMPORARY TABLE IF NOT EXISTS tmp_clean_intci (
                stage_id BIGINT PRIMARY KEY,
                v TEXT
            ) ENGINE=InnoDB
            """
        )
    conn.commit()


def clean_intci_main_name(conn, cfg: Config):
    """
    分批扫描 stage_id，清洗 intci_main_name（只处理 is_valid=1）
    写入断点表，支持断点续跑
    """
    ensure_progress_table(conn, cfg)
    ensure_tmp_clean_table(conn)

    last_id, updated_total, scanned_total = load_checkpoint(conn, cfg)
    print(f"[RESUME] last_stage_id={last_id}, scanned={scanned_total}, updated={updated_total}")

    while True:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT stage_id, intci_main_name
                FROM {cfg.stage_table}
                WHERE batch_id = %s
                  AND stage_id > %s
                  AND is_valid = 1
                  AND intci_main_name IS NOT NULL
                  AND intci_main_name <> ''
                  AND (LOCATE('$$$', intci_main_name) > 0 OR LOCATE('>', intci_main_name) > 0)
                ORDER BY stage_id
                LIMIT %s
                """,
                (cfg.batch_id, last_id, cfg.clean_batch_size),
            )
            rows = cur.fetchall()

        if not rows:
            break

        cleaned = []
        for row in rows:
            # DictCursor: row is dict; normal cursor: row is tuple
            if isinstance(row, dict):
                sid = row["stage_id"]
                raw = row["intci_main_name"]
            else:
                sid, raw = row[0], row[1]

            newv = deduplicate_by_level_fast(raw)
            if newv != raw:
                cleaned.append((sid, newv))
            last_id = sid

        if cleaned:
            with conn.cursor() as cur:
                cur.execute("TRUNCATE TABLE tmp_clean_intci")
                cur.executemany("INSERT INTO tmp_clean_intci (stage_id, v) VALUES (%s,%s)", cleaned)
                cur.execute(
                    f"""
                    UPDATE {cfg.stage_table} s
                    JOIN tmp_clean_intci t ON s.stage_id = t.stage_id
                    SET s.intci_main_name = t.v,
                        s.updated_date = CURRENT_TIMESTAMP
                    """
                )
            conn.commit()

        scanned_total += len(rows)
        updated_total += len(cleaned)
        save_checkpoint(conn, cfg, last_id, updated_total, scanned_total)

        print(
            f"[CLEAN] scanned={scanned_total}, last_stage_id={last_id}, "
            f"updated_total={updated_total}, updated_this_batch={len(cleaned)}"
        )

    print(f"[CLEAN-DONE] batch_id={cfg.batch_id}, scanned={scanned_total}, updated={updated_total}")


# =========================
# 5) 重复判脏（保留最小 stage_id）
# =========================
def build_dedup_sql(cfg: Config):
    create_tmp = """
    CREATE TEMPORARY TABLE IF NOT EXISTS tmp_dup_stage_ids (
      stage_id BIGINT PRIMARY KEY
    ) ENGINE=InnoDB
    """
    truncate_tmp = "TRUNCATE TABLE tmp_dup_stage_ids"

    insert_tmp = f"""
    INSERT INTO tmp_dup_stage_ids(stage_id)
    SELECT s.stage_id
    FROM {cfg.stage_table} s
    JOIN (
      SELECT patent_id, MIN(stage_id) AS keep_stage_id, COUNT(*) AS cnt
      FROM {cfg.stage_table}
      WHERE batch_id = %s
        AND is_valid = 1
      GROUP BY patent_id
      HAVING cnt > 1
    ) x
      ON s.patent_id = x.patent_id
    WHERE s.batch_id = %s
      AND s.is_valid = 1
      AND s.stage_id <> x.keep_stage_id
    """

    update_seg = f"""
    UPDATE {cfg.stage_table} s
    JOIN tmp_dup_stage_ids d ON s.stage_id = d.stage_id
    SET s.is_valid = 0,
        s.invalid_reason =
          CASE
            WHEN s.invalid_reason IS NULL OR TRIM(s.invalid_reason)='' THEN '重复patent_id'
            WHEN LOCATE('重复patent_id', s.invalid_reason) > 0 THEN s.invalid_reason
            ELSE CONCAT(s.invalid_reason, ';重复patent_id')
          END,
        s.updated_date = CURRENT_TIMESTAMP
    WHERE s.batch_id = %s
      AND s.stage_id BETWEEN %s AND %s
    """

    return create_tmp, truncate_tmp, insert_tmp, update_seg


def build_tmp_dup_ids_segmented(conn, cfg):
    """
    正确做法：
    1) 先算全 batch 的 keep_id（patent_id -> MIN(stage_id)）
    2) 再按 stage_id 分段，把需要置脏的 t.stage_id 写入 tmp_dup_stage_ids
    避免：每段自己算 keep_id 导致跨段保留多条的 bug
    """
    STAGE = cfg.stage_table
    TMP_DUP = 'tmp_dup_stage_ids'
    TMP_KEEP = getattr(cfg, "tmp_keep_table", "tmp_keep_patent_id")
    SEG = cfg.dedup_segment

    # 1) 建 keep 表（patent_id -> keep_id）
    with conn.cursor() as cur:
        cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {TMP_KEEP} (
          patent_id VARCHAR(255) COLLATE utf8mb4_unicode_ci NOT NULL,
          keep_id BIGINT NOT NULL,
          PRIMARY KEY (patent_id),
          KEY idx_keep_id (keep_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)
        cur.execute(f"TRUNCATE TABLE {TMP_KEEP}")

        # 全 batch 计算：只保留 patent_id 重复集合
        cur.execute(
            f"""
            INSERT INTO {TMP_KEEP} (patent_id, keep_id)
            SELECT patent_id, MIN(stage_id) AS keep_id
            FROM {STAGE}
            WHERE batch_id=%s AND is_valid=1
              AND patent_id IS NOT NULL AND TRIM(patent_id) <> ''
            GROUP BY patent_id
            HAVING COUNT(*) > 1
            """,
            (cfg.batch_id,)
        )
    conn.commit()

    # 2) 获取 stage_id 范围（注意：字段名就是 stage_id）
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT MIN(stage_id) mi, MAX(stage_id) ma
            FROM {STAGE}
            WHERE batch_id=%s AND is_valid=1
            """,
            (cfg.batch_id,)
        )
        row = cur.fetchone()
        mi, ma = row["mi"], row["ma"]

    if not mi or not ma:
        print("[DEDUP] no rows to process")
        return

    mi, ma = int(mi), int(ma)
    print(f"[DEDUP] build tmp_dup segmented stage_id_range=[{mi},{ma}] seg={SEG}")

    # 3) 分段插入 dup stage_id：JOIN keep 表，排除 keep_id
    l = mi
    while l <= ma:
        r = min(l + SEG - 1, ma)

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
                  AND s.stage_id <> k.keep_id
                """,
                (cfg.batch_id, l, r)
            )

        conn.commit()
        print(f"[DEDUP] tmp_dup seg [{l},{r}] rows={cur.rowcount}")
        l = r + 1

def dedup_mark_dirty_segmented_big(conn, cfg: Config):
    """
    大数据集可跑版本：
    1) TEMP keep 表：patent_id -> 全局最小 stage_id（分段 upsert + LEAST）
    2) TEMP dup 表：需要置脏的 stage_id（分段 insert ignore）
    3) 分段 UPDATE stage：join dup 表，置 is_valid=0 + 追加 invalid_reason
    """

    STAGE = cfg.stage_table
    BATCH = cfg.batch_id

    # 你现在 dedup_segment=200k，我建议先降到 50k 或 20k 更稳
    SEG = getattr(cfg, "dedup_segment", 50_000)

    REASON = "重复patent_id"

    TMP_KEEP = "tmp_keep_patent_id"
    TMP_DUP = "tmp_dup_stage_ids"

    # 0) 建临时表 & 清空
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

    run_with_retries(_init_tmp)

    # 1) 获取 stage_id 范围（只扫一次）
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
        print("[DEDUP] no rows to process")
        return

    mi, ma = int(mi), int(ma)
    print(f"[DEDUP] range=[{mi},{ma}] seg={SEG}")

    # 2) 分段构建 keep（patent_id -> 全局最小 stage_id）
    l = mi
    while l <= ma:
        r = min(l + SEG - 1, ma)

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

        run_with_retries(_build_keep_seg)
        print(f"[DEDUP] keep seg [{l},{r}] done")
        l = r + 1

    # 3) 分段写入 dup stage_id（join keep，排除 keep）
    l = mi
    total_dup = 0
    while l <= ma:
        r = min(l + SEG - 1, ma)

        def _build_dup_seg():
            nonlocal total_dup
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
                affected = cur.rowcount
            conn.commit()
            total_dup += max(0, affected)

        run_with_retries(_build_dup_seg)
        print(f"[DEDUP] dup seg [{l},{r}] inserted={total_dup}")
        l = r + 1

    # 4) 分段 UPDATE：join dup 表置脏（只更新本段 stage_id）
    l = mi
    total_updated = 0
    while l <= ma:
        r = min(l + SEG - 1, ma)

        def _update_seg():
            nonlocal total_updated
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
                    WHERE s.batch_id = %s
                      AND s.is_valid = 1
                      AND s.stage_id BETWEEN %s AND %s
                    """,
                    (REASON, REASON, REASON, BATCH, l, r),
                )
                aff = cur.rowcount
            conn.commit()
            total_updated += max(0, aff)

        run_with_retries(_update_seg)
        print(f"[DEDUP] update seg [{l},{r}] affected={total_updated}")
        l = r + 1

    print(f"[DEDUP DONE] dup_rows≈{total_dup}, updated={total_updated}")



def fix_stage_country_province_segmented(conn, stage_table: str, segment: int = 200_000):
    """
    对 stage 表执行三步修复（按 id 分段）：
    1) 境外省份为空 → 填国家中文名
    2) appl_country = 'WO' → appl_province 置 NULL
    3) HK/TW/MO → appl_country 统一改 CN
    """

    STAGE = stage_table

    # ===== 第一步：境外省份 =====
    sql_fix_foreign = f"""
    UPDATE {STAGE} s
    JOIN dict_country d
      ON s.appl_country COLLATE utf8mb4_0900_ai_ci
       = d.country_code COLLATE utf8mb4_0900_ai_ci
    SET s.appl_province = d.country_name_cn
    WHERE s.stage_id BETWEEN %s AND %s
      AND s.is_valid = 1
      AND s.appl_country <> 'CN'
      AND (s.appl_province IS NULL OR TRIM(s.appl_province) = '')
    """

    # ===== 第二步：WO =====
    sql_fix_wo = f"""
    UPDATE {STAGE}
    SET appl_province = NULL
    WHERE stage_id BETWEEN %s AND %s
      AND is_valid = 1
      AND appl_country = 'WO'
    """

    # ===== 第三步：HK / TW / MO =====
    sql_fix_hktwmo = f"""
    UPDATE {STAGE}
    SET appl_country = 'CN'
    WHERE stage_id BETWEEN %s AND %s
      AND is_valid = 1
      AND appl_country IN ('HK','TW','MO')
    """

    # signory号去浮点
    sql_fix_signory_item = f"""
    UPDATE {STAGE} s
    SET s.signory_item = CAST(CAST(TRIM(s.signory_item) AS DECIMAL(30,10)) AS UNSIGNED)
    WHERE s.stage_id BETWEEN %s AND %s
      AND is_valid = 1
      AND s.signory_item IS NOT NULL
      AND TRIM(s.signory_item) REGEXP '^[0-9]+\\\\.0+$'
    """

    # 更新application_origin 字段
    sql_application_origin = f"""
    UPDATE {STAGE} s
    SET s.application_origin =
      CASE
        WHEN s.appl_country = 'WO' THEN 'PCT国际申请'
        WHEN s.appl_country = 'CN' THEN '境内专利'
        WHEN s.appl_country IS NULL OR TRIM(s.appl_country) = '' THEN NULL
        ELSE '境外专利'
      END
    WHERE s.stage_id BETWEEN %s AND %s
      AND s.is_valid = 1
    """

    # ===== 取 id 范围 =====
    with conn.cursor() as cur:
        cur.execute(f"SELECT MIN(stage_id) AS mi, MAX(stage_id) AS ma FROM {STAGE}")
        row = cur.fetchone()

    min_id, max_id = row["mi"], row["ma"]
    if not min_id or not max_id:
        print(f"[STAGE_FIX] {STAGE} is empty, skip.")
        return

    print(f"[STAGE_FIX] table={STAGE}, id_range=[{min_id}, {max_id}], segment={segment}")

    l = min_id
    total1 = total2 = total3 = total4= total5 = 0

    while l <= max_id:
        r = min(l + segment - 1, max_id)

        try:
            with conn.cursor() as cur:
                cur.execute(sql_fix_foreign, (l, r))
                c1 = cur.rowcount

                cur.execute(sql_fix_wo, (l, r))
                c2 = cur.rowcount

                cur.execute(sql_fix_hktwmo, (l, r))
                c3 = cur.rowcount

                cur.execute(sql_fix_signory_item, (l, r))
                c4 = cur.rowcount

                cur.execute(sql_application_origin, (l, r))
                c5 = cur.rowcount



            conn.commit()
            total1 += max(c1, 0)
            total2 += max(c2, 0)
            total3 += max(c3, 0)
            total4 += max(c4, 0)
            total5 += max(c5, 0)

            print(
                f"[STAGE_FIX-OK] id[{l},{r}] "
                f"foreign={c1}, wo={c2}, hktwmo={c3},signory={c4} appl_type={c5} | "
                f"total=({total1},{total2},{total3},{total4},{total5})"
            )

        except Exception as e:
            conn.rollback()
            print(f"[STAGE_FIX-ROLLBACK] id[{l},{r}] err={e}")
            raise

        l = r + 1

    print(
        f"[STAGE_FIX-DONE] foreign={total1}, wo={total2}, hktwmo={total3} signory={total4} appl_type={total5}"
    )


# =========================
# 6) 总调度
# =========================
def run_job(cfg: Config):
    conn = connect_db(cfg)
    try:
        clean_intci_main_name(conn, cfg)
        # dedup_mark_dirty_segmented_big(conn, cfg)
        # print("[JOB DONE] clean_intci_main_name + dedup_mark_dirty done.")
        # fix_stage_country_province_segmented(conn,stage_table=Config.stage_table,segment=cfg.dedup_segment)

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    run_job(Config())
