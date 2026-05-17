import re
import time
import traceback
import pymysql
# ========= 基本参数 =========
DB = dict(
    host="172.16.0.60",
    port=13306,
    user="root",
    password="KpsMysql666",
    database="mdc_pat",
    charset="utf8mb4",
    autocommit=False,
)

#
ODS_TABLE = "ods_pat_raw_a_batch_01"          # ods_file_load_log.table_name
STAGE_TABLE = "dwd_patents_stage_a"             # ✅ 可替换为其它中间表
TABLE_ID = "a"                                # dict_pid_s3_map.table_id

# 跑批控制
FETCH_FILES_PER_ROUND = 50                    # 每轮从 ods_file_load_log 拉多少文件
COMMIT_EVERY_FILES = 5                        # 每多少个文件提交一次（=1 即单文件事务）
MODE = "all_not_success"                         # new / retry_failed / all_not_success
DEGRADE_TO_SINGLE_ON_GROUP_FAIL = True        # ✅ 组失败自动拆成单文件
DELETE_BEFORE_INSERT = False                  # 幂等开关：插入前删该批文件（谨慎使用）

# 打印控制
PRINT_EVERY_FILE_DETAIL = True                # 成功后逐文件打印 rows_loaded/rows_inserted/diff
PRINT_SQL_ON_ERROR = False                    # 出错时是否打印SQL（可能很长）


# ========= 你的清洗 SQL（你自己维护） =========
# 注意三点：
# 1) FROM {ods_table} t
# 2) m.table_id = %(table_id)s
# 3) AND t._source_file IN ({source_file_list})
CLEAN_SQL_TEMPLATE = r"""
INSERT INTO __STAGE_TABLE__ (
  batch_id, src_table, _source_file, _row_num,
  is_valid, invalid_reason,
  patent_id, patent_name, patent_type,
  legal_status, legal_detail_status, language,
  patent_no, application_date, application_year,
  applicant, publication_no, publication_date,
  publication_year, intci_code, intci_main_code, intci_main_name,
  agency, agent, inventor, is_fulltext, publish_file,
  pid, s3_path, file_size, s3_type, bucket_name,
  applicant_address, applicant_zipcode, appl_country, appl_province,
  abstract, signory_item, valid_status, created_by, updated_by,
  application_date_raw, publication_date_raw, intci_name_raw,
  patent_type_raw, legal_status_raw, language_raw
)
SELECT
  t.batch_id,
  t.src_table,
  t._source_file,
  t._row_num,
  t.is_valid,
  t.invalid_reason,
  t.patent_id,
  t.patent_name,
  t.patent_type,
  s.standard_value AS legal_status,
  t.legal_detail_status,
  COALESCE(u1.iso639_1, u2.iso639_1) AS language,
  t.patent_no,
  t.application_date,
  t.application_year,
  t.applicant,
  t.publication_no,
  t.publication_date,
  t.publication_year,

  CASE
    WHEN t.patent_type = '外观设计' THEN '无'
    ELSE NULLIF(REGEXP_REPLACE(REPLACE(TRIM(t.intci_code), ',', ';'), '\\s*;\\s*', ';'), '')
  END AS intci_code,

  CASE
    WHEN t.patent_type = '外观设计' THEN '无'
    ELSE t.intci_main_code
  END AS intci_main_code,

  CASE
    WHEN t.patent_type = '外观设计' THEN '无'
    ELSE t.intci_main_name
  END AS intci_main_name,

  t.agency,
  t.agent,
  t.inventor,
  t.is_fulltext,
  t.publish_file,

  m.pid AS pid,

  CASE
    WHEN m.table_id = 'a' THEN
      CASE
        WHEN t.publish_file IS NULL OR TRIM(t.publish_file) = '' THEN NULL
        ELSE CONCAT(m.s3_path_base, REGEXP_REPLACE(t.publish_file, '^files/', ''))
      END
    ELSE
      CASE
        WHEN t.publish_file IS NULL OR TRIM(t.publish_file) = '' THEN NULL
        ELSE CONCAT(m.s3_path_base, t.publish_file)
      END
  END AS s3_path,

  NULL AS file_size,
  m.s3_type AS s3_type,
  m.bucket_name AS bucket_name,

  t.applicant_address,
  t.applicant_zipcode,
  t.appl_country,
  t.appl_province,
  t.abstract,
  t.signory_item,
  t.valid_status,
  t.created_by,
  t.updated_by,

  t.application_date_raw,
  t.publication_date_raw,
  t.intci_name_raw,
  t.patent_type_raw,
  t.legal_status_raw,
  t.language_raw

FROM (
  SELECT
    batch_id,
    %s AS src_table,
    _source_file,
    _row_num,

    CASE
      WHEN id IS NULL OR TRIM(id) = '' THEN 0
      WHEN hasfulltext IS NULL OR hasfulltext = '0' THEN 0
      WHEN publish_file IS NULL THEN 0
      ELSE 1
    END AS is_valid,

    TRIM(BOTH ';' FROM CONCAT(
      CASE WHEN id IS NULL OR TRIM(id) = '' THEN 'patent_id为空;' ELSE '' END,
      CASE WHEN hasfulltext IS NULL OR TRIM(hasfulltext) = '' OR TRIM(hasfulltext) = '0'
           THEN 'hasfulltext为0或为空;' ELSE '' END,
      CASE WHEN (TRIM(hasfulltext) = '1') AND (publish_file IS NULL OR TRIM(publish_file) = '')
           THEN 'hasfulltext是1但publish_file为空;' ELSE '' END
    )) AS invalid_reason,

    id AS patent_id,
    TRIM(title) AS patent_name,
    patenttype AS patent_type,
    legalstatus AS legal_detail_status,
    language AS language,

    NULLIF(REGEXP_REPLACE(UPPER(TRIM(patentcode)), '^CN', ''), '') AS patent_no,

    CASE
      WHEN applicationdate IS NULL OR TRIM(applicationdate) = '' THEN NULL
      WHEN TRIM(applicationdate) IN ('0000-00-00','0000-00-00 00:00:00') THEN NULL
      ELSE
        CASE
          WHEN TRIM(applicationdate) REGEXP '^[0-9]{8}$'
            THEN STR_TO_DATE(TRIM(applicationdate), '%Y%m%d')

          WHEN LEFT(REPLACE(REPLACE(TRIM(applicationdate), '/', '-'), 'T', ' '), 10)
               REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
            THEN STR_TO_DATE(
                   LEFT(REPLACE(REPLACE(TRIM(applicationdate), '/', '-'), 'T', ' '), 10),
                   '%Y-%m-%d'
                 )

          WHEN REPLACE(REPLACE(TRIM(applicationdate), '/', '-'), 'T', ' ')
               REGEXP '^[0-9]{4}-[0-9]{1,2}-[0-9]{1,2}([ ].*)?$'
            THEN DATE(
                   STR_TO_DATE(
                     REPLACE(REPLACE(TRIM(applicationdate), '/', '-'), 'T', ' '),
                     '%Y-%c-%e %H:%i:%s'
                   )
                 )

          WHEN TRIM(applicationdate) REGEXP '^[0-9]{10}$'
            THEN DATE(FROM_UNIXTIME(CAST(TRIM(applicationdate) AS UNSIGNED)))
          WHEN TRIM(applicationdate) REGEXP '^[0-9]{13}$'
            THEN DATE(FROM_UNIXTIME(CAST(TRIM(applicationdate) AS UNSIGNED) DIV 1000))
          ELSE NULL
        END
    END AS application_date,

    LEFT(applicationyear, 4) AS application_year,

    NULLIF(REGEXP_REPLACE(REPLACE(TRIM(applicant), ',', ';'), '\\s*;\\s*', ';'), '') AS applicant,

    NULLIF(UPPER(TRIM(publicationno)), '') AS publication_no,

    CASE
      WHEN publicationdate IS NULL OR TRIM(publicationdate) = '' THEN NULL
      WHEN TRIM(publicationdate) IN ('0000-00-00','0000-00-00 00:00:00') THEN NULL
      ELSE
        CASE
          WHEN TRIM(publicationdate) REGEXP '^[0-9]{8}$'
            THEN STR_TO_DATE(TRIM(publicationdate), '%Y%m%d')

          WHEN LEFT(REPLACE(REPLACE(TRIM(publicationdate), '/', '-'), 'T', ' '), 10)
               REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
            THEN STR_TO_DATE(
                   LEFT(REPLACE(REPLACE(TRIM(publicationdate), '/', '-'), 'T', ' '), 10),
                   '%Y-%m-%d'
                 )

          WHEN REPLACE(REPLACE(TRIM(publicationdate), '/', '-'), 'T', ' ')
               REGEXP '^[0-9]{4}-[0-9]{1,2}-[0-9]{1,2}([ ].*)?$'
            THEN DATE(
                   STR_TO_DATE(
                     REPLACE(REPLACE(TRIM(publicationdate), '/', '-'), 'T', ' '),
                     '%Y-%c-%e %H:%i:%s'
                   )
                 )

          WHEN TRIM(publicationdate) REGEXP '^[0-9]{10}$'
            THEN DATE(FROM_UNIXTIME(CAST(TRIM(publicationdate) AS UNSIGNED)))
          WHEN TRIM(publicationdate) REGEXP '^[0-9]{13}$'
            THEN DATE(FROM_UNIXTIME(CAST(TRIM(publicationdate) AS UNSIGNED) DIV 1000))
          ELSE NULL
        END
    END AS publication_date,

    LEFT(publishyear, 4) AS publication_year,

    classcodeList AS intci_code,
    mainclasscode AS intci_main_code,
    mainclassname AS intci_main_name,
    agency AS agency,
    NULLIF( REGEXP_REPLACE(REPLACE(REPLACE(TRIM(agent), ',', ';'), '%', ';'),'\\s*;\\s*', ';'), '') AS agent,
    NULLIF(REGEXP_REPLACE(REPLACE(TRIM(inventor), ',', ';'), '\\s*;\\s*', ';'), '') AS inventor,

    CASE
      WHEN UPPER(TRIM(hasfulltext)) IN ('1') THEN 1
      WHEN UPPER(TRIM(hasfulltext)) IN ('0') THEN 0
      ELSE NULL
    END AS is_fulltext,

    publish_file AS publish_file,

    CASE
      WHEN applicantaddress REGEXP '^[0-9]{6}'
        THEN TRIM(SUBSTRING(applicantaddress, 7))
      ELSE applicantaddress
    END AS applicant_address,

    CASE
      WHEN applicantaddress REGEXP '^[0-9]{6}'
        THEN REGEXP_SUBSTR(applicantaddress, '^[0-9]{6}')
      ELSE ''
    END AS applicant_zipcode,

    countryorganization AS appl_country,

    CASE
      WHEN applicantarea IS NULL OR TRIM(applicantarea) = '' THEN NULL
      WHEN LOCATE(';', applicantarea) > 0 THEN LEFT(applicantarea, LOCATE(';', applicantarea) - 1)
      ELSE applicantarea
    END AS appl_province,

    abstract AS abstract,
    signoryitem AS signory_item,
    validity AS valid_status,
    created_by AS created_by,
    updated_by AS updated_by,

    applicationdate AS application_date_raw,
    publicationdate AS publication_date_raw,
    mainclassname AS intci_name_raw,
    patenttype AS patent_type_raw,
    legalstatus AS legal_status_raw,
    language AS language_raw

  FROM __ODS_TABLE__
) t

LEFT JOIN dict_mapping s
  ON t.legal_detail_status = s.source_value
 AND s.dict_code = 'legal_status'
 AND s.is_valid = 1

LEFT JOIN dict_language u1
  ON t.language = u1.iso639_2

LEFT JOIN dict_language u2
  ON t.language = u2.iso639_2_b

JOIN dict_pid_s3_map m
  ON m.table_id = %s

WHERE t._source_file IN (__SOURCE_FILE_LIST__)
"""




STAGE_DONE_DDL = """
CREATE TABLE IF NOT EXISTS stage_file_done (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  table_name VARCHAR(128) NOT NULL,
  batch_id VARCHAR(64) NULL,
  stage_table VARCHAR(128) NOT NULL,
  file_name VARCHAR(255) NOT NULL,
  status ENUM('SUCCESS','FAILED') NOT NULL,
  rows_loaded BIGINT NULL,
  rows_inserted BIGINT NULL,
  err_msg TEXT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uk_done (table_name, stage_table, batch_id, file_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

# =========================
# 3) 工具函数
# =========================

def now_ms() -> int:
    return int(time.time() * 1000)

def ms_to_s(ms: int) -> str:
    return f"{ms/1000:.2f}s"

def escape_percent_in_quoted_literals(sql: str) -> str:
    """
    只把单引号字符串常量里的 % 替换为 %%，避免 STR_TO_DATE(...,'%Y%m%d') 被 PyMySQL 误判。
    不会影响 SQL 外面的 %s 参数占位符。
    """
    def repl(m):
        inner = m.group(1)
        if "%" in inner:
            inner = inner.replace("%", "%%")
        return "'" + inner + "'"
    return re.sub(r"'([^']*)'", repl, sql)

def chunk(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

# =========================
# 4) DB 操作函数
# =========================

def ensure_done_table(conn):
    with conn.cursor() as cur:
        cur.execute(STAGE_DONE_DDL)
    conn.commit()

def fetch_file_rows(conn, limit_n: int):
    """
    返回：[{file_name, batch_id, rows_loaded}, ...]
    MODE:
      - new: 只取 ods_file_load_log SUCCESS 且 stage_file_done 没有 SUCCESS 记录的
      - retry_failed: 只取 stage_file_done FAILED
      - all_not_success: ods_file_load_log SUCCESS 且 (没记录 或 非SUCCESS)
    """
    if MODE == "retry_failed":
        sql = """
        SELECT d.file_name, d.batch_id, d.rows_loaded
        FROM stage_file_done d
        WHERE d.table_name=%s AND d.stage_table=%s AND d.status='FAILED'
        ORDER BY d.updated_at ASC, d.file_name ASC
        LIMIT %s
        """
        params = (ODS_TABLE, STAGE_TABLE, limit_n)

    elif MODE == "all_not_success":
        sql = """
        SELECT l.file_name, l.batch_id, l.rows_loaded
        FROM ods_file_load_log l
        LEFT JOIN stage_file_done d
          ON d.table_name=l.table_name
         AND d.stage_table=%s
         AND d.batch_id=l.batch_id
         AND d.file_name=l.file_name
         AND d.status='SUCCESS'
        WHERE l.table_name=%s
          AND l.status='SUCCESS'
          AND d.id IS NULL
        ORDER BY l.updated_at ASC, l.file_name ASC
        LIMIT %s
        """
        params = (STAGE_TABLE, ODS_TABLE, limit_n)

    else:  # MODE == "new"
        sql = """
        SELECT l.file_name, l.batch_id, l.rows_loaded
        FROM ods_file_load_log l
        LEFT JOIN stage_file_done d
          ON d.table_name=l.table_name
         AND d.stage_table=%s
         AND d.batch_id=l.batch_id
         AND d.file_name=l.file_name
         AND d.status='SUCCESS'
        WHERE l.table_name=%s
          AND l.status='SUCCESS'
          AND d.id IS NULL
        ORDER BY l.updated_at ASC, l.file_name ASC
        LIMIT %s
        """
        params = (STAGE_TABLE, ODS_TABLE, limit_n)

    with conn.cursor(pymysql.cursors.DictCursor) as cur:
        cur.execute(sql, params)
        return cur.fetchall()

def mark_done(conn, file_rows, status, rows_inserted_map=None, err_msg=None):
    """
    file_rows: [{'file_name','batch_id','rows_loaded'}, ...]
    rows_inserted_map: {(batch_id,file_name): rows_inserted}
    """
    rows_inserted_map = rows_inserted_map or {}

    sql = """
    INSERT INTO stage_file_done
      (table_name, batch_id, stage_table, file_name, status, rows_loaded, rows_inserted, err_msg)
    VALUES
      (%s,%s,%s,%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE
      status=VALUES(status),
      rows_loaded=VALUES(rows_loaded),
      rows_inserted=VALUES(rows_inserted),
      err_msg=VALUES(err_msg),
      updated_at=CURRENT_TIMESTAMP
    """
    with conn.cursor() as cur:
        for r in file_rows:
            f = r["file_name"]
            b = r.get("batch_id")
            rl = r.get("rows_loaded")
            ri = rows_inserted_map.get((b, f))
            cur.execute(sql, (ODS_TABLE, b, STAGE_TABLE, f, status, rl, ri, err_msg))


def fetch_rows_inserted_map(conn, stage_table: str, file_rows: list[dict]) -> dict:
    """
    一次性统计本组文件在 stage 中的实际行数（脏+净全量）
    返回 {(batch_id, file_name): rows_inserted}
    """
    if not file_rows:
        return {}

    tuple_placeholders = ",".join(["(%s,%s)"] * len(file_rows))
    sql = f"""
    SELECT batch_id, _source_file, COUNT(*) AS c
    FROM {stage_table}
    WHERE (batch_id, _source_file) IN ({tuple_placeholders})
    GROUP BY batch_id, _source_file
    """

    params = []
    for r in file_rows:
        params.extend([r["batch_id"], r["file_name"]])

    with conn.cursor(pymysql.cursors.DictCursor) as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()

    mp = {(x["batch_id"], x["_source_file"]): int(x["c"]) for x in rows}

    # 没查到的补 0，方便稽核
    for r in file_rows:
        mp.setdefault((r["batch_id"], r["file_name"]), 0)

    return mp


def delete_stage_by_files(conn, file_rows):
    """
    幂等删除：按 batch_id + _source_file 删除 stage 旧数据
    """
    if not file_rows:
        return 0
    # 逐文件删最稳（避免拼复杂 OR）
    deleted = 0
    with conn.cursor() as cur:
        for r in file_rows:
            cur.execute(
                f"DELETE FROM {STAGE_TABLE} WHERE batch_id=%s AND _source_file=%s",
                (r.get("batch_id"), r["file_name"])
            )
            deleted += cur.rowcount
    return deleted

def count_stage_rows(conn, batch_id: str, file_name: str) -> int:
    sql = f"SELECT COUNT(*) AS c FROM {STAGE_TABLE} WHERE batch_id=%s AND _source_file=%s"
    with conn.cursor(pymysql.cursors.DictCursor) as cur:
        cur.execute(sql, (batch_id, file_name))
        return int(cur.fetchone()["c"])

# =========================
# 5) 核心：执行 INSERT...SELECT
# =========================

def build_sql_for_files(ods_table: str, stage_table: str, files: list[str]) -> str:
    placeholders = ",".join(["%s"] * len(files))
    sql = (CLEAN_SQL_TEMPLATE
           .replace("__ODS_TABLE__", ods_table)
           .replace("__STAGE_TABLE__", stage_table)
           .replace("__SOURCE_FILE_LIST__", placeholders))
    sql = escape_percent_in_quoted_literals(sql)
    return sql

def run_insert_for_files(conn, ods_table: str, stage_table: str, table_id: str, files: list[str]) -> int:
    """
    注意：这里假设你的 SQL 里 %s 参数出现顺序是：
      1) table_id（CASE WHEN m.table_id = %s）
      2) ods_table 字符串（src_table = %s）
      3) table_id（JOIN dict_pid_s3_map m ON m.table_id = %s）
      4...) files IN (%s,%s,...)
    如果你 SQL 的 %s 顺序不同，你要相应调整 params 顺序。
    """
    sql = build_sql_for_files(ods_table, stage_table, files)
    params = [ ods_table, table_id] + files

    with conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.rowcount

# =========================
# 6) 跑批主流程（含降级单文件）
# =========================

def process_group(conn, group_rows):
    """
    返回：success(bool), inserted_total(int), rows_inserted_map(dict), err_msg(str|None)
    """
    group_files = [x["file_name"] for x in group_rows]
    t0 = now_ms()

    if DELETE_BEFORE_INSERT:
        deleted = delete_stage_by_files(conn, group_rows)
        print(f"  [IDEMPOTENT] deleted_stage_rows={deleted}")

    inserted_total = run_insert_for_files(conn, ODS_TABLE, STAGE_TABLE, TABLE_ID, group_files)

    # 成功后，逐文件 count stage 行数（用于稽核）
    rows_inserted_map = {}
    for r in group_rows:
        b = r.get("batch_id")
        f = r["file_name"]
        rows_inserted_map[(b, f)] = count_stage_rows(conn, b, f)

    dt = now_ms() - t0
    return True, inserted_total, rows_inserted_map, None, dt

def main():
    # 启动前校验：避免跑错 SQL
    required_marks = ["__ODS_TABLE__", "__STAGE_TABLE__", "__SOURCE_FILE_LIST__"]
    missing = [m for m in required_marks if m not in CLEAN_SQL_TEMPLATE]
    if missing:
        raise ValueError(f"CLEAN_SQL_TEMPLATE 缺少标记：{missing}；必须包含 {required_marks}")

    conn = pymysql.connect(**DB)
    try:
        ensure_done_table(conn)

        total_ok = 0
        total_fail = 0
        round_no = 0

        while True:
            round_no += 1
            file_rows = fetch_file_rows(conn, FETCH_FILES_PER_ROUND)
            if not file_rows:
                print(f"[DONE] all done. success_files={total_ok}, failed_files={total_fail}, mode={MODE}, stage={STAGE_TABLE}")
                break

            print(f"\n[ROUND] {round_no} fetched_files={len(file_rows)} commit_every_files={COMMIT_EVERY_FILES} mode={MODE} stage={STAGE_TABLE}")

            for group_rows in chunk(file_rows, COMMIT_EVERY_FILES):
                group_files = [x["file_name"] for x in group_rows]
                group_batch_ids = sorted({x.get("batch_id") for x in group_rows if x.get("batch_id")})

                print(f"\n[GROUP] files={len(group_rows)} batch_ids={group_batch_ids[:3]}{'...' if len(group_batch_ids) > 3 else ''}")
                if len(group_files) <= 10:
                    print(f"  files={group_files}")
                else:
                    print(f"  files(head)={group_files[:5]} ... files(tail)={group_files[-2:]}")

                # ========== 组执行 ==========
                try:
                    if DELETE_BEFORE_INSERT:
                        deleted = delete_stage_by_files(conn, group_rows)
                        print(f"  [IDEMPOTENT] deleted_stage_rows={deleted}")

                    inserted_total = run_insert_for_files(conn, ODS_TABLE, STAGE_TABLE, TABLE_ID, group_files)

                    # ✅ 一次性统计每个文件 rows_inserted（stage事实行数：脏+净全量）
                    rows_inserted_map = fetch_rows_inserted_map(conn, STAGE_TABLE, group_rows)

                    mark_done(conn, group_rows, "SUCCESS", rows_inserted_map=rows_inserted_map, err_msg=None)
                    conn.commit()

                    total_ok += len(group_rows)
                    print(f"[SUCCESS] group_files={len(group_rows)} inserted_total={inserted_total}")

                    if PRINT_EVERY_FILE_DETAIL:
                        for r in group_rows:
                            f = r["file_name"]
                            b = r.get("batch_id")
                            rl = r.get("rows_loaded")
                            ri = rows_inserted_map.get((b, f))
                            diff = None if rl is None or ri is None else int(rl) - int(ri)
                            print(f"  - file={f} | batch_id={b} | rows_loaded={rl} | rows_inserted={ri} | diff={diff}")

                except Exception as e:
                    conn.rollback()
                    msg = f"{type(e).__name__}: {e}"
                    print(f"[FAILED] group_files={len(group_rows)} err={msg}")

                    # ========== 组失败降级单文件 ==========
                    if DEGRADE_TO_SINGLE_ON_GROUP_FAIL and len(group_rows) > 1:
                        print("  [DEGRADE] group failed -> retry each file individually...")

                        for one in group_rows:
                            f = one["file_name"]
                            b = one.get("batch_id")
                            try:
                                if DELETE_BEFORE_INSERT:
                                    deleted = delete_stage_by_files(conn, [one])
                                    print(f"    [IDEMPOTENT] file={f} deleted_stage_rows={deleted}")

                                inserted_total_1 = run_insert_for_files(conn, ODS_TABLE, STAGE_TABLE, TABLE_ID, [f])

                                rows_inserted_map_1 = fetch_rows_inserted_map(conn, STAGE_TABLE, [one])

                                mark_done(conn, [one], "SUCCESS", rows_inserted_map=rows_inserted_map_1, err_msg=None)
                                conn.commit()

                                total_ok += 1

                                rl = one.get("rows_loaded")
                                ri = rows_inserted_map_1.get((b, f))
                                diff = None if rl is None or ri is None else int(rl) - int(ri)

                                print(f"    [OK] file={f} | batch_id={b} | inserted_total={inserted_total_1} | rows_loaded={rl} | rows_inserted={ri} | diff={diff}")

                            except Exception as e2:
                                conn.rollback()
                                msg2 = f"{type(e2).__name__}: {e2}"
                                mark_done(conn, [one], "FAILED", rows_inserted_map=None, err_msg=msg2[:2000])
                                conn.commit()

                                total_fail += 1
                                print(f"    [FAIL] file={f} | batch_id={b} | err={msg2}")

                    else:
                        # 不降级：整组标记失败
                        mark_done(conn, group_rows, "FAILED", rows_inserted_map=None, err_msg=msg[:2000])
                        conn.commit()
                        total_fail += len(group_rows)

    finally:
        conn.close()

if __name__ == "__main__":
    main()