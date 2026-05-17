# -*- coding: utf-8 -*-
import time
from typing import Tuple

from ..db.mysql import connect


def get_id_range(conn, table, where_sql, params=()) -> Tuple[int, int]:
    sql = f"SELECT MIN(id), MAX(id) FROM {table} WHERE {where_sql}"
    with conn.cursor() as cur:
        cur.execute(sql, params)
        row = cur.fetchone()
        # 兼容 tuple cursor / DictCursor
        if isinstance(row, dict):
            mn, mx = row.get("MIN(id)"), row.get("MAX(id)")
        else:
            mn, mx = row
    return (mn or 0), (mx or 0)


def run_finalize_unqualified(cfg: dict):
    """
    完整搬运自：插入unqualified.py
    不改 SQL 字段与顺序，只做参数化接入 cfg。
    """
    ds = cfg["dataset"]
    run = cfg.get("run", {})

    # 你原脚本手动改的：DATA_SOURCE / CHUNK / 表名
    DATA_SOURCE = ds.get("data_source")
    CHUNK = int(run.get("chunk", 50_000))

    IN_STOCK_C = ds["instock_table"]                # 例如 dwd_patents_in_stock_d
    UNQUAL_C = ds["unqualified_table"]              # 例如 dwd_patents_unqualified_d
    UNQUAL_FINAL = ds.get("final_unqualified_table", "dwd_patents_unqualified")

    DEFAULT_CREATED_BY = "etl"
    DEFAULT_UPDATED_BY = "etl"

    def step1_insert_instockc_to_unqualified_c(conn, l, r):
        """
        dwd_patents_in_stock_c (is_valid=0) -> dwd_patents_unqualified_c
        字段基本同构，额外补：invalid_reason / batch_id 等源表有
        """
        sql = f"""
        INSERT IGNORE INTO {UNQUAL_C} (
          id, batch_id, src_table, _source_file, _row_num,
          is_valid, invalid_reason,
          patent_id, patent_name, patent_type, legal_status, legal_detail_status, language,
          patent_no, application_date, application_year, applicant,
          publication_no, publication_date, publication_year,
          intci_code, intci_main_code, intci_main_name,
          agency, agent, inventor,
          is_fulltext, publish_file, pid, s3_path, file_size, s3_type, bucket_name,
          applicant_address, applicant_zipcode, application_origin, appl_country, appl_province,
          abstract, signory_item, valid_status,
          created_by, updated_by
        )
        SELECT
          s.id, s.batch_id, s.src_table, s._source_file, s._row_num,
          s.is_valid, s.invalid_reason,
          s.patent_id, s.patent_name, s.patent_type, s.legal_status, s.legal_detail_status, s.language,
          s.patent_no, s.application_date, s.application_year, s.applicant,
          s.publication_no, s.publication_date, s.publication_year,
          s.intci_code, s.intci_main_code, s.intci_main_name,
          s.agency, s.agent, s.inventor,
          s.is_fulltext, s.publish_file, s.pid, s.s3_path, s.file_size, s.s3_type, s.bucket_name,
          s.applicant_address, s.applicant_zipcode, s.application_origin, s.appl_country, s.appl_province,
          s.abstract, s.signory_item, s.valid_status,
          COALESCE(s.created_by, %s) AS created_by,
          COALESCE(s.updated_by, %s) AS updated_by
        FROM {IN_STOCK_C} s
        WHERE s.is_valid = 0
          AND s.id BETWEEN %s AND %s
        """
        with conn.cursor() as cur:
            cur.execute(sql, (DEFAULT_CREATED_BY, DEFAULT_UPDATED_BY, l, r))
            return cur.rowcount

    def step2_insert_unqualified_c_to_final(conn, l, r):
        """
        dwd_patents_unqualified_c -> dwd_patents_unqualified
        invalid_reason -> reason
        目标表一些字段长度更短：patent_name(767), intci_code(255), agent(50), inventor(255), appl_province(10)
        用 LEFT(...) 截断，避免 Data too long 报错（这点非常关键）
        """
        sql = f"""
        INSERT IGNORE INTO {UNQUAL_FINAL} (
          id,
          patent_id,
          patent_name,
          patent_type,
          legal_status,
          legal_detail_status,
          `LANGUAGE`,
          patent_no,
          application_date,
          application_year,
          applicant,
          publication_no,
          publication_date,
          publish_year,
          intci_code,
          intci_main_code,
          intci_main_name,
          agency,
          agent,
          inventor,
          is_fulltext,
          publish_file,
          pid,
          s3_path,
          file_size,
          s3_type,
          bucket_name,
          applicant_address,
          applicant_zipcode,
          appl_country,
          appl_province,
          reason,
          abstract,
          signory_item,
          valid_status,
          data_source,
          created_by,
          updated_by
        )
        SELECT
          u.id,
          u.patent_id,                        -- 目标表 patent_id NOT NULL
          u.patent_name,
          u.patent_type,
          u.legal_status,
          u.legal_detail_status,
          u.language AS `LANGUAGE`,
          u.patent_no,
          u.application_date,
          u.application_year,
          u.applicant,
          u.publication_no,
          u.publication_date,
          u.publication_year AS publish_year,
          u.intci_code,
          u.intci_main_code,
          u.intci_main_name,
          u.agency,
          u.agent,
          u.inventor,
          u.is_fulltext,
          u.publish_file,
          u.pid,
          u.s3_path,
          u.file_size,
          u.s3_type,
          u.bucket_name,
          u.applicant_address,
          u.applicant_zipcode,
          u.appl_country,
          u.appl_province,
          u.invalid_reason AS reason,
          u.abstract,
          u.signory_item,
          u.valid_status,
          %s AS data_source,
          COALESCE(u.created_by, %s) AS created_by,
          COALESCE(u.updated_by, %s) AS updated_by
        FROM {UNQUAL_C} u
        WHERE u.id BETWEEN %s AND %s
        """
        with conn.cursor() as cur:
            cur.execute(sql, (DATA_SOURCE, DEFAULT_CREATED_BY, DEFAULT_UPDATED_BY, l, r))
            return cur.rowcount

    conn = connect(cfg["mysql"])
    try:
        # STEP 1 range：in_stock_c is_valid=0
        mn1, mx1 = get_id_range(conn, IN_STOCK_C, "is_valid=0")
        print(f"[STEP1 RANGE] {IN_STOCK_C} is_valid=0 id=[{mn1},{mx1}] chunk={CHUNK}")

        total1 = 0
        l = mn1
        while l <= mx1 and mx1 > 0:
            r = min(l + CHUNK - 1, mx1)
            t0 = time.time()
            aff = step1_insert_instockc_to_unqualified_c(conn, l, r)
            conn.commit()
            total1 += max(0, aff)
            print(f"[STEP1] id=[{l},{r}] affected={aff} total={total1} cost={time.time()-t0:.1f}s")
            l = r + 1

        # STEP 2 range：unqualified_c（刚灌入的或已有的）按 id 全量推进
        mn2, mx2 = get_id_range(conn, UNQUAL_C, "1=1")
        print(f"[STEP2 RANGE] {UNQUAL_C} id=[{mn2},{mx2}] chunk={CHUNK}")

        total2 = 0
        l = mn2
        while l <= mx2 and mx2 > 0:
            r = min(l + CHUNK - 1, mx2)
            t0 = time.time()
            aff = step2_insert_unqualified_c_to_final(conn, l, r)
            conn.commit()
            total2 += max(0, aff)
            print(f"[STEP2] id=[{l},{r}] affected={aff} total={total2} cost={time.time()-t0:.1f}s")
            l = r + 1

        print(f"[DONE] step1_inserted={total1}, step2_inserted={total2}")

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
