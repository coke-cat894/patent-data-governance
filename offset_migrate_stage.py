# -*- coding: utf-8 -*-
"""
offset_migrate_stage_d.py

目标：
- 将 D 分区的 stage_id / in_stock_d.id / unqualified_d.id
  整体 + offset，使其接在 C 分区后面
- 分段执行，避免大事务
- 支持断点续跑：可随时中断、重跑不会二次平移

核心规则：
- offset_d = MAX(stage_c.stage_id)   （让 D 接在 C 后）
- d_old_max = MAX(stage_d.stage_id WHERE stage_id < offset_d)
  这样即便 D 已经跑过一部分（新 id 变大），也不会污染 old_max

后续使用：
- 以次类推，目前id排序顺序是 A-C-D，若后续有新的一批专利数据，取D最大的offset值做整体平移。需要手动调整下面所有的表名。
"""

import time
import pymysql


DB_CONF = dict(
    host="172.16.0.60",
    port=13306,
    user="root",
    password="KpsMysql666",
    database="mdc_pat",
    charset="utf8mb4",
    cursorclass=pymysql.cursors.DictCursor,
    autocommit=False,
)

CHUNK_SIZE = 100_000   # 每段 10 万（稳）
SLEEP_SEC = 0.2


# 1) 取 offset：C 当前最大 stage_id
SQL_OFF_D = "SELECT COALESCE(MAX(stage_id), 0) AS off_d FROM dwd_patents_stage_c"



# 2) 取 D 的 old_max：只看“旧区间”(stage_id < offset)
SQL_D_OLD_MAX = """
SELECT COALESCE(MAX(stage_id), 0) AS d_old_max
FROM dwd_patents_stage_d
WHERE stage_id < %(off_d)s
"""

# 3) 断点续跑：找仍在旧区间内的最小 stage_id
SQL_NEXT_L_D = """
SELECT MIN(stage_id) AS next_l
FROM dwd_patents_stage_d
WHERE stage_id <= %(d_old_max)s
"""

# 4) 更新三张表
SQL_UPDATE_STAGE_D = """
UPDATE dwd_patents_stage_d
SET stage_id = stage_id + %(offset)s
WHERE stage_id BETWEEN %(l)s AND %(r)s
  AND stage_id <= %(d_old_max)s
"""

SQL_UPDATE_INSTOCK_D = """
UPDATE dwd_patents_in_stock_d
SET id = id + %(offset)s
WHERE id BETWEEN %(l)s AND %(r)s
  AND id <= %(d_old_max)s
"""

SQL_UPDATE_UNQUALIFIED_D = """
UPDATE dwd_patents_unqualified_d
SET id = id + %(offset)s
WHERE id BETWEEN %(l)s AND %(r)s
  AND id <= %(d_old_max)s
"""


def main():
    conn = pymysql.connect(**DB_CONF)

    try:
        with conn.cursor() as cur:
            # offset_d：让 D 接在 C 后面
            cur.execute(SQL_OFF_D)
            off_d = cur.fetchone()["off_d"]

            if off_d <= 0:
                raise RuntimeError("off_d=0：stage_c 为空？请先确认 C 是否已平移且有数据。")

            # D 的 old_max（只取旧区间 < off_d，避免被已平移的新值污染）
            cur.execute(SQL_D_OLD_MAX, {"off_d": off_d})
            d_old_max = cur.fetchone()["d_old_max"]

            print(f"[INFO] offset_d(off_d)={off_d:,}, d_old_max={d_old_max:,}")

            if d_old_max <= 0:
                print("[DONE] D 表看起来没有旧区间数据需要平移（可能已平移完 / 或 D 为空）")
                return

            # 断点续跑：找到还没平移的最小旧 stage_id
            cur.execute(SQL_NEXT_L_D, {"d_old_max": d_old_max})
            next_l = cur.fetchone()["next_l"]

            if next_l is None:
                print("[DONE] D 旧区间已清空，无需执行")
                return

            l = int(next_l)

            # 可选：打印剩余旧区间行数（做个心里有数）
            cur.execute(
                "SELECT COUNT(*) AS cnt FROM dwd_patents_stage_d WHERE stage_id <= %s",
                (d_old_max,),
            )
            left_cnt = cur.fetchone()["cnt"]
            print(f"[INFO] rows_left_in_old_range={left_cnt:,}, resume_from_l={l:,}")

        # 分段更新
        while l <= d_old_max:
            r = min(l + CHUNK_SIZE - 1, d_old_max)

            with conn.cursor() as cur:
                cur.execute(SQL_UPDATE_STAGE_D, {
                    "offset": off_d,
                    "l": l,
                    "r": r,
                    "d_old_max": d_old_max,
                })
                s_stage = cur.rowcount

                cur.execute(SQL_UPDATE_INSTOCK_D, {
                    "offset": off_d,
                    "l": l,
                    "r": r,
                    "d_old_max": d_old_max,
                })
                s_in = cur.rowcount

                cur.execute(SQL_UPDATE_UNQUALIFIED_D, {
                    "offset": off_d,
                    "l": l,
                    "r": r,
                    "d_old_max": d_old_max,
                })
                s_un = cur.rowcount

            conn.commit()

            # 你之前 C 的日志验证过：stage = in_stock + unqualified
            # 这里也建议打印出来方便你盯数据
            print(
                f"[OK] D [{l:,} ~ {r:,}] | "
                f"stage={s_stage}, in_stock={s_in}, unqualified={s_un}, "
                f"sum(in+un)={s_in + s_un}"
            )

            l = r + 1
            time.sleep(SLEEP_SEC)

        print("[DONE] D 分区偏移完成")

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
