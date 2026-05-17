"""
Stage -> DWD split runner (starter).

Migration target: 同时分流.py
"""
from ._shared import get_dataset_cfg, get_run_cfg
import time
import pymysql
from pymysql.err import OperationalError


def run_stage_split(cfg: dict):
    ds = get_dataset_cfg(cfg)
    run = get_run_cfg(cfg)
    progress_table = cfg.get("progress", {}).get("stage_split_progress_table", "etl_stage_split_progress")

    stage_table = ds["stage_table"]
    batch_id = ds["batch_id"]
    valid_table = ds["instock_table"]
    invalid_table = ds["unqualified_table"]
    segment = int(run.get("segment", 200_000))

    max_retries = int(run.get("max_retries", 5))

    RETRY_SLEEP = 10

    job_valid = f"split_in_stock_{ds.get('table_id','x')}_v1"
    job_invalid = f"split_unqualified_{ds.get('table_id','x')}_v1"

    print(f"[split] stage={stage_table} -> valid={valid_table} invalid={invalid_table} batch_id={batch_id} segment={segment}")
    print(f"       jobs: {job_valid} / {job_invalid} progress_table={progress_table}")
    print("TODO: paste logic from 同时分流.py into this function.")

    from ..db.mysql import connect as _connect
    def connect():
        return _connect(cfg["mysql"])

    def ensure_progress_table(conn):
        with conn.cursor() as cur:
            cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {progress_table} (
              stage_table   VARCHAR(128) NOT NULL,
              target_table  VARCHAR(128) NOT NULL,
              batch_id      VARCHAR(64)  NOT NULL,
              job_name      VARCHAR(64)  NOT NULL,
              last_r_stage_id BIGINT     NOT NULL DEFAULT 0,
              inserted_rows BIGINT       NOT NULL DEFAULT 0,
              updated_rows  BIGINT       NOT NULL DEFAULT 0,
              affected_rows BIGINT       NOT NULL DEFAULT 0,
              updated_date  TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP
                            ON UPDATE CURRENT_TIMESTAMP,
              PRIMARY KEY(stage_table, target_table, batch_id, job_name)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """)
        conn.commit()

    def load_checkpoint(conn, target_table: str, job_name: str):
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT last_r_stage_id, inserted_rows, updated_rows, affected_rows
                FROM {progress_table}
                WHERE stage_table=%s AND target_table=%s AND batch_id=%s AND job_name=%s
            """, (stage_table, target_table, batch_id, job_name))
            row = cur.fetchone()
        if not row:
            return 0, 0, 0, 0
        return int(row[0]), int(row[1]), int(row[2]), int(row[3])

    def save_checkpoint(conn, target_table: str, job_name: str, last_r: int, ins: int, upd: int, aff: int):
        with conn.cursor() as cur:
            cur.execute(f"""
                INSERT INTO {progress_table}
                  (stage_table, target_table, batch_id, job_name, last_r_stage_id, inserted_rows, updated_rows, affected_rows)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                ON DUPLICATE KEY UPDATE
                  last_r_stage_id=VALUES(last_r_stage_id),
                  inserted_rows=VALUES(inserted_rows),
                  updated_rows=VALUES(updated_rows),
                  affected_rows=VALUES(affected_rows),
                  updated_date=CURRENT_TIMESTAMP
            """, (stage_table, target_table, batch_id, job_name, last_r, ins, upd, aff))
        conn.commit()

    def get_min_max_stage_id(conn, is_valid: int):
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT COALESCE(MIN(stage_id),0), COALESCE(MAX(stage_id),0)
                FROM {stage_table}
                WHERE batch_id=%s AND is_valid=%s
            """, (batch_id, is_valid))
            mn, mx = cur.fetchone()
        return int(mn), int(mx)

    def build_insert_sql_valid(target_table: str) -> str:
        # 合格表：不包含 invalid_reason
        return f"""
        INSERT INTO {target_table} (
          id, batch_id, src_table, _source_file, _row_num,
          is_valid,invalid_reason,
          patent_id, patent_name, patent_type,
          legal_status, legal_detail_status, `language`,
          patent_no, application_date, application_year,
          applicant, publication_no, publication_date, publication_year,
          intci_code, intci_main_code, intci_main_name,
          agency, agent, inventor,
          is_fulltext, publish_file,
          pid, s3_path, file_size, s3_type, bucket_name,
          applicant_address, applicant_zipcode, application_origin,appl_country, appl_province,
          abstract, signory_item, valid_status,
          created_by, created_date, updated_by, updated_date
        )
        SELECT s.stage_id,
          s.batch_id, s.src_table, s._source_file, s._row_num,
          s.is_valid AS is_valid,
          invalid_reason,
          s.patent_id,
          COALESCE(NULLIF(TRIM(s.patent_name), ''), '') AS patent_name,
          NULLIF(TRIM(s.patent_type), '')         AS patent_type,
          NULLIF(TRIM(s.legal_status), '')        AS legal_status,
          NULLIF(TRIM(s.legal_detail_status), '') AS legal_detail_status,
          s.`language`,
          s.patent_no,
          s.application_date,
          s.application_year,
          s.applicant,
          s.publication_no,
          s.publication_date,
          s.publication_year,
          s.intci_code,
          s.intci_main_code,
          s.intci_main_name,
          s.agency,
          s.agent,
          s.inventor,
          s.is_fulltext,
          s.publish_file,
          s.pid,
          s.s3_path,
          ROUND(s.file_size / 1024 /1024, 3) AS file_size,
          s.s3_type,
          s.bucket_name,
          s.applicant_address,
          s.applicant_zipcode,
          s.application_origin,
          s.appl_country,
          s.appl_province,
          s.abstract,
          s.signory_item,
          s.valid_status,
          s.created_by,
          s.created_date,
          s.updated_by,
          s.updated_date
        FROM {stage_table} s FORCE INDEX (idx_stage_batch_valid_stageid)
        WHERE s.batch_id = %s
          AND s.is_valid = 1
          AND s.stage_id BETWEEN %s AND %s
          AND s.patent_id IS NOT NULL
          AND TRIM(s.patent_id) <> ''
        ORDER BY s.stage_id;
        """

    def build_insert_sql_invalid(target_table: str) -> str:
        # 不合格表：额外写入 invalid_reason（来自 stage.invalid_reason）
        return f"""
        INSERT INTO {target_table} (
          id,batch_id, src_table, _source_file, _row_num,
          is_valid,
          invalid_reason,
          patent_id, patent_name, patent_type,
          legal_status, legal_detail_status, `language`,
          patent_no, application_date, application_year,
          applicant, publication_no, publication_date, publication_year,
          intci_code, intci_main_code, intci_main_name,
          agency, agent, inventor,
          is_fulltext, publish_file,
          pid, s3_path, file_size, s3_type, bucket_name,
          applicant_address, applicant_zipcode, application_origin,appl_country, appl_province,
          abstract, signory_item, valid_status,
          created_by, created_date, updated_by, updated_date
        )
        SELECT s.stage_id,
          s.batch_id, s.src_table, s._source_file, s._row_num,
          s.is_valid AS is_valid,
          s.invalid_reason,
          s.patent_id,
          COALESCE(NULLIF(TRIM(s.patent_name), ''), '') AS patent_name,
          NULLIF(TRIM(s.patent_type), '')         AS patent_type,
          NULLIF(TRIM(s.legal_status), '')        AS legal_status,
          NULLIF(TRIM(s.legal_detail_status), '') AS legal_detail_status,
          s.`language`,
          s.patent_no,
          s.application_date,
          s.application_year,
          s.applicant,
          s.publication_no,
          s.publication_date,
          s.publication_year,
          s.intci_code,
          s.intci_main_code,
          s.intci_main_name,
          s.agency,
          s.agent,
          s.inventor,
          s.is_fulltext,
          s.publish_file,
          s.pid,
          s.s3_path,
          s.file_size,
          s.s3_type,
          s.bucket_name,
          s.applicant_address,
          s.applicant_zipcode,
          s.application_origin,
          s.appl_country,
          s.appl_province,
          s.abstract,
          s.signory_item,
          s.valid_status,
          s.created_by,
          s.created_date,
          s.updated_by,
          s.updated_date
        FROM {stage_table} s FORCE INDEX (idx_stage_batch_valid_stageid)
        WHERE s.batch_id = %s
          AND s.is_valid = 0
          AND s.stage_id BETWEEN %s AND %s
        ORDER BY s.stage_id;
        """

    def run_one_segment(conn, insert_sql: str, l_id: int, r_id: int) -> int:
        with conn.cursor() as cur:
            cur.execute(insert_sql, (batch_id, l_id, r_id))
            return cur.rowcount

    def run_flow(conn, *, target_table: str, job_name: str, stage_is_valid: int, out_is_valid: int):
        """
        跑一条分流链路（合格或不合格）
        """
        if stage_is_valid == 1:
            insert_sql = build_insert_sql_valid(target_table)
        else:
            insert_sql = build_insert_sql_invalid(target_table)

        last_r, ins_total, upd_total, aff_total = load_checkpoint(conn, target_table, job_name)
        mn, mx = get_min_max_stage_id(conn, stage_is_valid)

        if mx == 0:
            print(f"[EMPTY] stage has no rows for batch_id={batch_id} is_valid={stage_is_valid}")
            return

        start = max(mn, last_r + 1)
        print(
            f"[START] target={target_table} is_valid={stage_is_valid} stage_id_range=[{mn},{mx}] resume_from={start} step={segment}")

        l_id = start
        while l_id <= mx:
            r_id = min(l_id + segment - 1, mx)
            attempt = 0

            while True:
                attempt += 1
                t0 = time.time()
                try:
                    affected = run_one_segment(conn, insert_sql, l_id, r_id)
                    conn.commit()

                    aff_total += max(0, affected)
                    save_checkpoint(conn, target_table, job_name, r_id, ins_total, upd_total, aff_total)

                    dt = time.time() - t0
                    print(
                        f"[OK] target={target_table} seg=[{l_id},{r_id}] affected={affected} total_affected={aff_total} cost={dt:.1f}s")
                    break

                except OperationalError as e:
                    conn.rollback()
                    msg = str(e)
                    retryable = any(code in msg for code in ["1205", "1213", "2006", "2013", "08S01"])
                    print(f"[ERR] target={target_table} seg=[{l_id},{r_id}] attempt={attempt} err={msg}")

                    if (not retryable) or attempt >= max_retries:
                        raise

                    if "2006" in msg or "2013" in msg or "08S01" in msg:
                        try:
                            conn.close()
                        except Exception:
                            pass
                        time.sleep(2)
                        conn = connect()
                        ensure_progress_table(conn)

                    time.sleep(RETRY_SLEEP)

            l_id = r_id + 1

        print(f"[DONE] target={target_table} is_valid={stage_is_valid} last_r={mx} total_affected={aff_total}")



    conn = connect()
    try:
        ensure_progress_table(conn)

        # 先跑合格，再跑不合格（顺序无所谓）
        run_flow(
            conn,
            target_table=valid_table,
            job_name=job_valid,
            stage_is_valid=1,
            out_is_valid=1,
        )

        run_flow(
            conn,
            target_table=invalid_table,
            job_name=job_invalid,
            stage_is_valid=0,
            out_is_valid=0,
        )

        print(f"[ALL-DONE] batch_id={batch_id} valid->{valid_table} invalid->{invalid_table}")

    finally:
        try:
            conn.close()
        except Exception:
            pass



