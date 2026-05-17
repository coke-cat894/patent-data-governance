import pymysql

HOST = "172.16.0.60"
PORT = 13306
USER = "root"
PASSWORD = "KpsMysql666"
DB = "mdc_pat"

BATCH_ID = "batch_20260108_153537"
SEGMENT = 200000

conn = pymysql.connect(
    host=HOST,
    port=PORT,
    user=USER,
    password=PASSWORD,
    database=DB,
    charset="utf8mb4",
    autocommit=False
)

try:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT MIN(stage_id), MAX(stage_id)
            FROM dwd_patents_stage_g
            WHERE batch_id=%s AND is_valid=1
        """, (BATCH_ID,))
        min_id, max_id = cur.fetchone()

    if not min_id:
        print("没有可回刷数据")
        exit()

    l = int(min_id)
    max_id = int(max_id)

    while l <= max_id:
        r = min(l + SEGMENT - 1, max_id)

        with conn.cursor() as cur:
            cur.execute(f"""
                UPDATE dwd_patents_in_stock_g i
                JOIN dwd_patents_stage_g s
                  ON i.id = s.stage_id
                SET i.intci_main_name = s.intci_main_name
                WHERE s.batch_id=%s
                  AND s.is_valid=1
                  AND s.stage_id BETWEEN %s AND %s
            """, (BATCH_ID, l, r))

            affected = cur.rowcount

        conn.commit()
        print(f"回刷区间 [{l}, {r}] -> 更新 {affected} 行")

        l = r + 1

finally:
    conn.close()