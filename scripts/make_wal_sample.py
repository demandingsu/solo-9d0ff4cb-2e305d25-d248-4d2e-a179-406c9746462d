"""频点测齐核查演示：生成采集开始时的主库与同轮 WAL。

场景（试验 #1「A墙-计权隔声-第1次」，必测频点 100/125/160/200 Hz）：

- 第 0 版（主库）：100 Hz 已测；
- 第 1 笔提交：补测 125 Hz；
- 第 2 笔提交：补测 160 Hz；
- 第 3 笔提交：**补测 200 Hz → 四个频点首次测齐**；
- 第 4 笔提交：把 200 Hz 的读数撤回（level_db 置空，笔记“复测前撤回”）；
- 第 5 笔提交：重测 200 Hz；
- 最后一笔是**未提交**的事务（写入 250 Hz），导出时仍可能残留其帧，
  关闭连接时回滚 —— 核查应忽略它。

因此：必测 100/125/160/200 → 第 3 版测齐；再加 250 → 一直没测齐，
最后一版缺 250（第 5 版 200 Hz 的编号是重测后的 105）。

输出（同一目录，可直接上传到 /check）：
    in_situ-main.sqlite          采集开始时的主库（第 0 版）
    in_situ-main.sqlite-wal      同轮、尚未回填的 WAL（含未提交事务的帧）
    in_situ-truncated.sqlite-wal 截断的 WAL（演示忽略截断尾部，
                                 只到第 1 版，必测 160/200 Hz 时未测齐）

用法：
    python3 scripts/make_wal_sample.py [输出目录，默认 sample_wal]

把主库与 WAL 一起上传到 /check，选试验 #1、填 100,125,160,200，
应报告「最早在第 3 版测齐」。
"""
from __future__ import annotations

import os
import shutil
import sqlite3
import sys

NAME = "in_situ"

DDL = """
CREATE TABLE groups (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL
);
CREATE TABLE tests (
    id INTEGER PRIMARY KEY,
    group_id INTEGER,
    name TEXT NOT NULL,
    standard TEXT,
    conducted_on TEXT
);
CREATE TABLE readings (
    id INTEGER PRIMARY KEY,
    test_id INTEGER NOT NULL REFERENCES tests(id),
    kind TEXT NOT NULL DEFAULT 'measurement',
    frequency_hz REAL,
    level_db REAL,
    note TEXT
);
CREATE INDEX idx_readings_test ON readings(test_id);
"""


def _commit(conn: sqlite3.Connection, *statements: tuple[str, tuple]) -> None:
    conn.execute("BEGIN")
    for sql, params in statements:
        conn.execute(sql, params)
    conn.execute("COMMIT")


def _reading(rid: int, freq: float, level: float | None, note: str | None = None):
    return (
        "INSERT INTO readings (id, test_id, kind, frequency_hz, level_db, note) "
        "VALUES (?, 1, 'measurement', ?, ?, ?)",
        (rid, freq, level, note),
    )


def create_wal_sample(out_dir: str) -> tuple[str, str]:
    os.makedirs(out_dir, exist_ok=True)
    live = os.path.join(out_dir, NAME + ".sqlite")
    for suffix in ("", "-wal", "-shm"):
        if os.path.exists(live + suffix):
            os.remove(live + suffix)

    conn = sqlite3.connect(live, isolation_level=None)  # 手动管理事务
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA wal_autocheckpoint=0")  # 不让提交被回填进主库

        conn.executescript(DDL)
        conn.execute("INSERT INTO groups VALUES (1, '墙体试件A')")
        conn.execute(
            "INSERT INTO tests VALUES (1, 1, 'A墙-计权隔声-第1次', 'GB/T 19889.3', '2026-03-02')"
        )
        # 第 0 版：100 Hz 已测，200 Hz 读数在扫频中被覆盖成空值
        conn.execute(
            "INSERT INTO readings (id, test_id, kind, frequency_hz, level_db, note) "
            "VALUES (101, 1, 'measurement', 100.0, 52.1, NULL)"
        )
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")  # 第 0 版完整落在主库

        # 导出「采集开始时」的主库
        main_copy = os.path.join(out_dir, NAME + "-main.sqlite")
        shutil.copyfile(live, main_copy)
        live_wal = live + "-wal"

        _commit(conn, _reading(102, 125.0, 51.0))                 # 第 1 版
        _commit(conn, _reading(103, 160.0, 49.8))                 # 第 2 版
        _commit(conn, _reading(104, 200.0, 47.2))                 # 第 3 版：首次测齐
        _commit(                                              # 第 4 版：撤回 200 Hz
            conn,
            (
                "UPDATE readings SET level_db = NULL, note = '复测前撤回' "
                "WHERE id = 104",
                (),
            ),
        )
        _commit(conn, _reading(105, 200.0, 47.0))                 # 第 5 版：重测

        # 未提交的事务：关闭时回滚，其帧仍可能留在 WAL 中
        conn.execute("BEGIN")
        conn.execute(
            "INSERT INTO readings (id, test_id, kind, frequency_hz, level_db) "
            "VALUES (106, 1, 'measurement', 250.0, 45.5)"
        )

        wal = main_copy + "-wal"
        shutil.copyfile(live_wal, wal)
    finally:
        conn.execute("ROLLBACK")
        conn.close()

    # 截断副本：保留第 1 笔提交的完整帧，截断第 2 笔提交的前两个帧，
    # 让页面能看到「截断的日志尾部被忽略」。此时只有第 0、1 版可核查。
    raw = open(wal, "rb").read()

    page_size = int.from_bytes(raw[8:12], "big") or 65536
    trunc = os.path.join(out_dir, NAME + "-truncated.sqlite-wal")
    cut = 32 + (24 + page_size) * 3 + (24 + page_size) // 2
    with open(trunc, "wb") as f:
        f.write(raw[:cut])

    # 清掉生成过程中的活动库，输出目录只留可上传的三个文件
    for suffix in ("", "-wal", "-shm"):
        try:
            os.remove(live + suffix)
        except OSError:
            pass
    return main_copy, wal


if __name__ == "__main__":
    out = sys.argv[1] if len(sys.argv) > 1 else "sample_wal"
    main_db, wal = create_wal_sample(out)
    print(f"主库：{main_db}")
    print(f"WAL：{wal}")
    print(f"截断 WAL：{os.path.join(out, NAME + '-truncated.sqlite-wal')}")
