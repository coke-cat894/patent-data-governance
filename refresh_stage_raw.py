import pymysql
import time

# ====== 连接配置（按你的环境改） ======
HOST = "172.16.0.60"
PORT = 13306
USER = "root"
PASSWORD = "KpsMysql666"
DB = "mdc_pat"
CHARSET = "utf8mb4"

# ====== 业务参数 ======
BATCH_ID = "batch_20260116_104541"
SEGMENT = 200000          # 每段更新的 stage_id 范围（建议 5万~20万）
SLEEP_SEC = 0.2           # 每段提交后稍微停一下，减少锁压力
MAX_RETRIES = 5           # 锁等待/死锁重试次数

STAGE_TABLE = "dwd_patents_stage_e"


RETRYABLE_ERRNOS = {1205, 1213, 2006, 2013}  # lock wait, deadlock, gone away, lost connection


def is_retryable(e: Exception) -> bool:
    return isinstance(e, pymysql.err.OperationalError) and e.args and e.args[0] in RETRYABLE_ERRNOS


def main():
    conn = pymysql.connect(
        host=HOST,
        port=PORT,
        user=USER,
        password=PASSWORD,
        database=DB,
        charset=CHARSET,
        autocommit=False,
    )

    try:
        # 1) 获取本批次可刷范围
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT MIN(stage_id), MAX(stage_id)
                FROM {STAGE_TABLE}
                WHERE batch_id=%s
                  AND is_valid=1
                  AND intci_name_raw IS NOT NULL
                  AND intci_name_raw <> ''
                """,
                (BATCH_ID,),
            )
            min_id, max_id = cur.fetchone()

        if min_id is None:
            print("没有可回刷的数据（可能 batch_id 不对或 intci_name_raw 为空）")
            return

        min_id, max_id = int(min_id), int(max_id)
        print(f"[RANGE] batch={BATCH_ID} stage_id=[{min_id},{max_id}] segment={SEGMENT}")

        # 2) 分段回刷
        l = min_id
        total = 0
        while l <= max_id:
            r = min(l + SEGMENT - 1, max_id)

            for attempt in range(1, MAX_RETRIES + 1):
                try:
                    with conn.cursor() as cur:
                        cur.execute(
                            f"""
                            UPDATE {STAGE_TABLE}
                            SET intci_main_name = intci_name_raw
                            WHERE batch_id=%s
                              AND is_valid=1
                              AND stage_id BETWEEN %s AND %s
                              AND intci_name_raw IS NOT NULL
                              AND intci_name_raw <> ''
                            """,
                            (BATCH_ID, l, r),
                        )
                        affected = cur.rowcount

                    conn.commit()
                    total += max(0, affected)
                    print(f"[OK] [{l},{r}] updated={affected} total={total}")
                    break

                except Exception as e:
                    conn.rollback()
                    if attempt == MAX_RETRIES or not is_retryable(e):
                        raise
                    print(f"[RETRY] [{l},{r}] attempt={attempt}/{MAX_RETRIES} err={e}")
                    time.sleep(1.0 * attempt)

            l = r + 1
            if SLEEP_SEC > 0:
                time.sleep(SLEEP_SEC)

        print(f"[DONE] total_updated={total}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()