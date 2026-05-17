import os
import glob
from datetime import datetime
import pymysql
import time

# =========================
# 1) 配置集中在这里（你后续换表只改这里）
# =========================
CONFIG = {
    "csv_dir": r"G:\c\csv",
    "table": "ods_pat_raw_c_batch_01",
    "cols": [
        "id","title","type","patenttype","legalstatus","hasfulltext","publish_file",
        "applicant","applicantaddress","countryorganization","applicantarea","patentcode",
        "applicationdate","applicationyear","publicationno","publicationdate","publishyear",
        "classcodeList","mainclasscode","mainclassname","agency","agent","inventor","abstract",
        "signoryitem","validity","LANGUAGE","priorityList","citedcount","pdf_url"
    ],
    "use_trace_cols": True,   # 表里必须有 _source_file/_row_num
    "field_term": ",",
    "line_term": "\n",        # 更稳：用 \n
    "commit_every": 50,       # ✅ 加速关键：每 N 个文件 commit 一次
    #日志记录表，不用更改。
    "log_table": "ods_file_load_log",

    # 重要：MySQL LOAD DATA 的字符集名
    # CSV 是 UTF-8 / UTF-8-SIG 时，MySQL 这里用 utf8（不是 utf-8）
    "mysql_charsets": ["utf8"],

    "mysql_conf": dict(
        host="172.16.0.60",
        port=13306,
        user="root",
        password="KpsMysql666",
        database="mdc_pat",
        charset="utf8mb4",
        local_infile=1,
        autocommit=False,
    ),
}


# =========================
# 2) 日志表（方案一）
# =========================
def ensure_log_table(cur, log_table: str):
    ddl = f"""
CREATE TABLE IF NOT EXISTS `{log_table}` (
  batch_id        VARCHAR(64)   NOT NULL,
  table_name      VARCHAR(128)  NOT NULL,
  file_name       VARCHAR(255)  NOT NULL,
  file_path       VARCHAR(1024) NOT NULL,
  file_size       BIGINT        NOT NULL,
  file_mtime      DATETIME      NOT NULL,
  status          ENUM('SUCCESS','FAILED') NOT NULL,
  encoding        VARCHAR(32)   NULL,
  rows_loaded     BIGINT        NULL,
  error_message   TEXT          NULL,
  created_at      TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at      TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (batch_id, table_name, file_name),
  KEY idx_status (batch_id, table_name, status),
  KEY idx_file (table_name, file_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""
    cur.execute(ddl)


def file_stat(csv_path: str):
    st = os.stat(csv_path)
    size = int(st.st_size)
    mtime_dt = datetime.fromtimestamp(st.st_mtime).strftime("%Y-%m-%d %H:%M:%S")
    return size, mtime_dt


def upsert_log(cur, log_table: str, batch_id: str, table_name: str, csv_path: str,
               status: str, encoding=None, rows_loaded=None, error_message=None):
    size, mtime_dt = file_stat(csv_path)
    sql = f"""
INSERT INTO `{log_table}`
  (batch_id, table_name, file_name, file_path, file_size, file_mtime,
   status, encoding, rows_loaded, error_message)
VALUES
  (%s, %s, %s, %s, %s, %s,
   %s, %s, %s, %s)
ON DUPLICATE KEY UPDATE
  file_path=VALUES(file_path),
  file_size=VALUES(file_size),
  file_mtime=VALUES(file_mtime),
  status=VALUES(status),
  encoding=VALUES(encoding),
  rows_loaded=VALUES(rows_loaded),
  error_message=VALUES(error_message);
"""
    cur.execute(sql, (
        batch_id, table_name,
        os.path.basename(csv_path), csv_path, size, mtime_dt,
        status, encoding, rows_loaded, error_message
    ))


def get_files_failed(cur, log_table: str, batch_id: str, table_name: str):
    sql = f"""
SELECT file_path
FROM `{log_table}`
WHERE batch_id=%s AND table_name=%s AND status='FAILED'
ORDER BY updated_at ASC;
"""
    cur.execute(sql, (batch_id, table_name))
    return [r[0] for r in cur.fetchall()]


def get_files_success_map(cur, log_table: str, batch_id: str, table_name: str):
    """
    dict: file_path -> (file_size, file_mtime_str)
    用于断点续跑：跳过 SUCCESS 且文件未变的文件
    """
    sql = f"""
SELECT file_path, file_size, DATE_FORMAT(file_mtime,'%%Y-%%m-%%d %%H:%%i:%%s')
FROM `{log_table}`
WHERE batch_id=%s AND table_name=%s AND status='SUCCESS';
"""
    cur.execute(sql, (batch_id, table_name))
    d = {}
    for fp, size, mtime_str in cur.fetchall():
        d[fp] = (int(size), mtime_str)
    return d


def scan_csv_files(csv_dir: str):
    return sorted([
        f for f in glob.glob(os.path.join(csv_dir, "*.csv"))
        if not os.path.basename(f).startswith("~$")
    ])


# =========================
# 3) 单文件导入（LOAD DATA LOCAL INFILE）
# =========================
def load_one_file(cur, cfg: dict, csv_path: str, batch_id: str):
    table_name = cfg["table"]
    cols = cfg["cols"]
    field_term = cfg["field_term"]
    line_term = cfg["line_term"]
    use_trace_cols = cfg["use_trace_cols"]
    mysql_charsets = cfg["mysql_charsets"]

    set_parts = [f"`batch_id` = '{batch_id}'"]
    if use_trace_cols:
        cur.execute("SET @rownum := 0;")
        set_parts.append(f"`_source_file` = '{os.path.basename(csv_path)}'")
        set_parts.append("`_row_num` = (@rownum := @rownum + 1)")

    set_sql = "SET " + ", ".join(set_parts)

    sql_tpl = f"""
LOAD DATA LOCAL INFILE %s
INTO TABLE `{table_name}`
CHARACTER SET {{charset}}
FIELDS TERMINATED BY %s
OPTIONALLY ENCLOSED BY '\"'
ESCAPED BY '\"'
LINES TERMINATED BY %s
IGNORE 1 LINES
({", ".join([f"`{c}`" for c in cols])})
{set_sql};
""".strip()

    last_err = None
    for cs in mysql_charsets:
        try:
            cur.execute(sql_tpl.format(charset=cs), (csv_path, field_term, line_term))
            cur.execute("SELECT ROW_COUNT()")
            rows_loaded = cur.fetchone()[0]
            return cs, rows_loaded
        except Exception as e:
            last_err = e
    raise last_err


# =========================
# 4) 主流程：full / resume / rerun_failed + 每 N 文件 commit
# =========================
def load_folder_to_ods(cfg: dict,
                       batch_id: str = None,
                       mode: str = "full",        # "full" | "resume" | "rerun_failed"
                       skip_success_unchanged: bool = True):
    if mode not in ("full", "resume", "rerun_failed"):
        raise ValueError("mode must be one of: full, resume, rerun_failed")

    batch_id = batch_id or ("batch_" + datetime.now().strftime("%Y%m%d_%H%M%S"))

    csv_dir = cfg["csv_dir"]
    table_name = cfg["table"]
    log_table = cfg["log_table"]
    commit_every = int(cfg["commit_every"])

    conn = pymysql.connect(**cfg["mysql_conf"])

    failed = []
    total_ok_files = 0

    start_ts = time.time()
    last_commit_ts = start_ts
    committed_files = 0

    try:
        with conn.cursor() as cur:
            # 1) 确保日志表存在
            ensure_log_table(cur, log_table)
            conn.commit()

            # 2) 会话级加速（一次即可）
            cur.execute("SET SESSION foreign_key_checks=0;")
            cur.execute("SET SESSION unique_checks=0;")
            conn.commit()

            print(f"[INFO] table={table_name} | batch_id={batch_id} | mode={mode} | commit_every={commit_every}")

            # 3) 生成要处理的文件列表
            if mode == "rerun_failed":
                files = get_files_failed(cur, log_table, batch_id, table_name)
                files = [f for f in files if os.path.exists(f)]
                print(f"[INFO] Failed files to rerun: {len(files)}")
            else:
                files = scan_csv_files(csv_dir)
                print(f"[INFO] CSV files found: {len(files)}")

            if not files:
                print("[INFO] No files to process.")
                return batch_id

            # 4) resume：跳过成功且未变文件
            if mode == "resume" and skip_success_unchanged:
                success_map = get_files_success_map(cur, log_table, batch_id, table_name)
                filtered = []
                skipped = 0
                for fp in files:
                    if fp in success_map:
                        size_now, mtime_now = file_stat(fp)
                        size_old, mtime_old = success_map[fp]
                        if size_now == size_old and mtime_now == mtime_old:
                            skipped += 1
                            continue
                    filtered.append(fp)
                files = filtered
                print(f"[INFO] Skipped unchanged success files: {skipped}")
                print(f"[INFO] Files to process now: {len(files)}")

            # 5) 导入：每 N 个文件 commit 一次
            pending = 0
            for fp in files:
                try:
                    charset_used, rows_loaded = load_one_file(cur, cfg, fp, batch_id)

                    upsert_log(
                        cur, log_table=log_table, batch_id=batch_id, table_name=table_name,
                        csv_path=fp, status="SUCCESS",
                        encoding=charset_used, rows_loaded=rows_loaded, error_message=None
                    )

                    pending += 1
                    total_ok_files += 1

                    if pending >= commit_every:
                        conn.commit()
                        committed_files += pending
                        now = time.time()
                        elapsed = now - start_ts
                        interval = now - last_commit_ts

                        print(
                            f"[COMMIT] {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | "
                            f"files_committed={committed_files} | "
                            f"last_batch_files={pending} | "
                            f"elapsed={elapsed / 60:.1f} min | "
                            f"since_last_commit={interval:.1f} s"
                        )
                        pending = 0
                        last_commit_ts = now

                    print(f"[DONE] {os.path.basename(fp)} | charset={charset_used} | rows={rows_loaded}")

                except Exception as e:
                    # 回滚当前未提交批次
                    conn.rollback()
                    pending = 0

                    # 失败日志单独记一条并提交
                    try:
                        upsert_log(
                            cur, log_table=log_table, batch_id=batch_id, table_name=table_name,
                            csv_path=fp, status="FAILED",
                            encoding=None, rows_loaded=None, error_message=str(e)
                        )
                        conn.commit()
                    except Exception as log_e:
                        conn.rollback()
                        print(f"[WARN] Failed to write log for {os.path.basename(fp)}: {log_e}")

                    failed.append((fp, str(e)))
                    print(f"[FAIL] {os.path.basename(fp)} | {e}")

            # 6) 最后补一次 commit
            if pending > 0:
                conn.commit()
                committed_files += pending
                now = time.time()
                elapsed = now - start_ts

                print(
                    f"[COMMIT-FINAL] {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | "
                    f"files_committed={committed_files} | "
                    f"last_batch_files={pending} | "
                    f"elapsed={elapsed / 60:.1f} min"
                )

            print(f"\n[SUMMARY] success_files={total_ok_files}, failed_files={len(failed)}")

            # 失败清单写文件（可选）
            if failed:
                out = os.path.join(csv_dir, f"failed_files_{batch_id}.txt")
                with open(out, "w", encoding="utf-8") as w:
                    for fp, err in failed:
                        w.write(fp + "\t" + err + "\n")
                print(f"[INFO] Failed file list saved to: {out}")

            return batch_id

    finally:
        conn.close()


# =========================
# 5) main：选择一种模式运行
# =========================
if __name__ == "__main__":
    # BATCH_ID = batch_20260108_140121
    # 1) 首次全量导入
    batch = load_folder_to_ods(CONFIG, mode="full")
    print("BATCH_ID =", batch)

    # 2) 中断/想续跑（跳过成功且未变）
    # load_folder_to_ods(CONFIG,batch_id='batch_20260108_145031', mode="resume", skip_success_unchanged=True)

    # 3) 只重跑失败文件（精准补齐）
    # load_folder_to_ods(CONFIG, batch_id=batch, mode="rerun_failed")
