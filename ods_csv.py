import pymysql
import csv

# ===============================
# 远程数据库连接信息
# ===============================
REMOTE = dict(
    host="172.16.0.60",
    port=13306,
    user="root",
    password="KpsMysql666",
    db="mdc_pat",
    charset="utf8mb4",
    cursorclass=pymysql.cursors.SSCursor  # 关键：流式游标
)

# ===============================
# 配置
# ===============================
REMOTE_TABLE = "ods_pat_raw_h_batch_01"   # TODO: 改成你要导出的表
OUTPUT_FILE = "patents_h.csv"
BATCH_SIZE = 5000


def main():
    conn = pymysql.connect(**REMOTE)

    try:
        with conn.cursor() as cur:
            print("获取字段结构...")
            cur.execute(f"SHOW COLUMNS FROM `{REMOTE_TABLE}`")
            columns = [row[0] for row in cur.fetchall()]

        print("开始导出数据...")

        with conn.cursor() as cur, open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)

            # 写入表头
            writer.writerow(columns)

            cur.execute(f"SELECT * FROM `{REMOTE_TABLE}`")

            count = 0

            while True:
                rows = cur.fetchmany(BATCH_SIZE)
                if not rows:
                    break

                writer.writerows(rows)
                count += len(rows)

                print(f"已导出 {count} 条")

        print(f"\n导出完成 ✅ 文件: {OUTPUT_FILE}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()