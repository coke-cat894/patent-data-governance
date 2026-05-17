import pymysql
import time

MYSQL = dict(
    host="172.16.0.60",
    port=13306,
    user="root",
    password="KpsMysql666",
    database="mdc_pat",
    charset="utf8mb4",
    autocommit=False,
)

SRC_TABLE = "dwd_patents_in_stock_a"
DST_TABLE = "dwd_patents_in_stock"
CHUNK = 50000

# 你说的：data_source 用变量，手动改
DATA_SOURCE = "A"

# 如果目标表 created_by / updated_by 是 NOT NULL，而源表可能为空，建议兜底
DEFAULT_CREATED_BY = "etl"
DEFAULT_UPDATED_BY = "etl"


def main():
    conn = pymysql.connect(**MYSQL)
    try:
        # 取源表 is_valid=1 的 id 范围
        with conn.cursor() as cur:
            cur.execute(f"SELECT MIN(id), MAX(id) FROM {SRC_TABLE} WHERE is_valid = 1")
            mn, mx = cur.fetchone()
            mn = mn or 0
            mx = mx or 0
            print(f"[RANGE] {SRC_TABLE} is_valid=1 id=[{mn},{mx}] chunk={CHUNK}")

        # 说明：
        # - 这里用 INSERT ... SELECT，速度快、网络开销低
        # - inti_main_type_code: COALESCE(NULLIF(LEFT(TRIM(intci_main_code),3),''),'UNK')
        # - publish_year 目标字段对应源表 publication_year
        # - data_source 固定变量
        SQL_INSERT_BATCH = f"""
        INSERT INTO {DST_TABLE} (
          id,
          patent_id,
          patent_name,
          patent_type,
          legal_status,
          legal_detail_status,
          `LANGUAGE`,
          application_origin,
          patent_no,
          application_date,
          application_year,
          applicant,
          publication_no,
          publication_date,
          publish_year,
          intci_code,
          intci_main_code,
          inti_main_type_code,
          intci_main_name,
          agency,
          agent,
          inventor,
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
          abstract,
          signory_item,
          valid_status,
          data_source,
          created_by,
          updated_by
        )
        SELECT
          s.id,
          s.patent_id,
          s.patent_name,
          s.patent_type,
          s.legal_status,
          s.legal_detail_status,
          s.language AS `LANGUAGE`,
          s.application_origin,
          s.patent_no,
          s.application_date,
          s.application_year,
          s.applicant,
          s.publication_no,
          s.publication_date,
          s.publication_year AS publish_year,
          s.intci_code,
          s.intci_main_code,
          COALESCE(NULLIF(LEFT(TRIM(s.intci_main_code), 3), ''), 'UNK') AS inti_main_type_code,
          s.intci_main_name,
          s.agency,
          s.agent,
          s.inventor,
          s.publish_file,
          s.pid,
          s.s3_path,
          s.file_size,
          s.s3_type,
          s.bucket_name,
          s.applicant_address,
          s.applicant_zipcode,
          s.appl_country,
          s.appl_province,
          s.abstract,
          s.signory_item,
          s.valid_status,
          %s AS data_source,
          COALESCE(s.created_by, %s) AS created_by,
          COALESCE(s.updated_by, %s) AS updated_by
        FROM {SRC_TABLE} s
        WHERE s.is_valid = 1
          AND s.id BETWEEN %s AND %s
        """

        total = 0
        l = mn
        while l <= mx:
            r = min(l + CHUNK - 1, mx)
            t0 = time.time()
            with conn.cursor() as cur:
                cur.execute(SQL_INSERT_BATCH, (DATA_SOURCE, DEFAULT_CREATED_BY, DEFAULT_UPDATED_BY, l, r))
                aff = cur.rowcount
            conn.commit()

            total += max(0, aff)
            print(f"[INSERT] id=[{l},{r}] affected={aff} total={total} cost={time.time()-t0:.1f}s")
            l = r + 1

        print(f"[DONE] inserted rows={total}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
