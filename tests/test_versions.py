"""频点测齐核查测试：WAL 解析、逐版状态、补测/覆盖/撤回、截断与未提交数据。"""
import os
import shutil
import sqlite3

import pytest

from app.versions import (
    AnalysisError,
    WalError,
    check_versions,
    parse_frequencies,
    parse_wal,
)
from scripts.make_wal_sample import create_wal_sample


@pytest.fixture()
def wal_sample(tmp_path):
    main_db, wal = create_wal_sample(str(tmp_path / "wal_sample"))
    return main_db, wal


@pytest.fixture()
def work(tmp_path):
    return str(tmp_path / "work.db")


def _report(main_db, wal, work, freqs=(100, 125, 160, 200), test_id=1):
    return check_versions(main_db, wal, work, test_id, list(freqs))


def test_parse_wal_commits(wal_sample):
    _main, wal = wal_sample
    log = parse_wal(wal)
    assert log.page_size == 4096
    assert log.version_count == 5
    assert log.uncommitted_frames == 0
    assert log.ignored_bytes == 0
    # 第 4 笔提交只改一页
    assert len(log.commits[3].frames) == 1


def test_first_complete_version(wal_sample, work):
    main_db, wal = wal_sample
    report = _report(main_db, wal, work)
    assert report.total_versions == 5
    assert report.first_complete is not None
    assert report.first_complete.version == 3
    assert report.first_complete.readings == {
        "100": 101,
        "125": 102,
        "160": 103,
        "200": 104,
    }
    assert report.wal_note is None


def test_retracted_reading_makes_later_versions_incomplete(wal_sample, work):
    """若不看第 3 版，只看撤回后的状态：第 4 版缺 200 Hz，第 5 版又测齐。"""
    main_db, wal = wal_sample
    # 用 250 Hz 一并作为必测点：从未测过 → 一直没测齐，报告最后一版缺口
    report = _report(main_db, wal, work, freqs=(100, 125, 160, 200, 250))
    assert report.first_complete is None
    assert report.last_state.version == 5
    assert report.last_state.missing == ["250"]
    assert report.last_state.readings["200"] == 105  # 重测后的读数


def test_main_alone_has_first_readings_only(wal_sample, work):
    """主库（第 0 版）：100 Hz 已测，其余未测。"""
    main_db, wal = wal_sample
    report = _report(main_db, wal, work, freqs=(100, 125))
    # 100 在第 0 版已测，但 125 要到第 1 版 → 最早第 1 版测齐
    assert report.first_complete.version == 1


def test_already_complete_in_main(wal_sample, work):
    main_db, wal = wal_sample
    report = _report(main_db, wal, work, freqs=(100,))
    assert report.first_complete.version == 0
    assert report.first_complete.readings == {"100": 101}


def test_truncated_tail_ignored(wal_sample, work):
    main_db, wal = wal_sample
    truncated = wal.replace("-main.sqlite-wal", "-truncated.sqlite-wal")
    report = _report(main_db, truncated, work)
    assert report.total_versions == 1
    assert report.wal_note and "截断" in report.wal_note
    assert report.ignored_bytes > 0
    assert report.first_complete is None
    assert report.last_state.version == 1
    assert report.last_state.missing == ["160", "200"]


def test_truncated_mid_commit_is_not_counted(wal_sample, tmp_path, work):
    """在提交帧之后截断：已提交的版本保留，其后的半帧不计入。"""
    main_db, wal = wal_sample
    raw = open(wal, "rb").read()
    page_size = int.from_bytes(raw[8:12], "big") or 65536
    fsize = 24 + page_size
    # 样例 WAL 每笔提交 2 帧（第 4 笔仅 1 帧）：保留前 6 帧 = 前 3 笔提交，
    # 再留下第 7 帧的帧头（不完整）
    cut = 32 + fsize * 6 + 24
    path = tmp_path / "cut.sqlite-wal"
    path.write_bytes(raw[:cut])
    report = _report(main_db, str(path), work)
    assert report.total_versions == 3
    assert report.first_complete.version == 3
    assert "截断" in (report.wal_note or "")


def test_truncated_at_commit_boundary(wal_sample, tmp_path, work):
    """正好在提交帧处截断：不留残余帧也无需告警，版本数正确。"""
    main_db, wal = wal_sample
    raw = open(wal, "rb").read()
    page_size = int.from_bytes(raw[8:12], "big") or 65536
    fsize = 24 + page_size
    path = tmp_path / "cut4.sqlite-wal"
    path.write_bytes(raw[: 32 + fsize * 4])  # 前 4 帧 = 2 笔提交
    report = _report(main_db, str(path), work)
    assert report.total_versions == 2
    assert report.wal_note is None
    assert report.first_complete is None
    assert report.last_state.version == 2
    assert report.last_state.missing == ["200"]


def test_uncommitted_frames_ignored(tmp_path, work):
    """WAL 末尾追加一帧无提交标记的有效帧：不计入版本，也不影响有效提交。"""
    main_db, wal = create_wal_sample(str(tmp_path / "s"))
    log = parse_wal(wal)
    raw = open(wal, "rb").read()
    page_size = log.page_size
    last = log.commits[-1].frames[-1]
    frame_off = last.data_offset - 24
    # 取最后一帧的数据，改成一个非提交帧（db_size 字段置 0）
    frame = bytearray(raw[frame_off : frame_off + 24 + page_size])
    frame[4:8] = b"\x00\x00\x00\x00"
    frame[8:16] = raw[16:24]

    from app.versions import _checksum

    s0, s1 = log.checksum_seed
    for commit in log.commits:
        for f in commit.frames:
            fh = raw[f.data_offset - 24 : f.data_offset]
            data = raw[f.data_offset : f.data_offset + page_size]
            s0, s1 = _checksum(fh[:8] + data, s0, s1, log.checksum_order)
    c0, c1 = _checksum(frame[:8] + frame[24:], s0, s1, log.checksum_order)
    frame[16:20] = c0.to_bytes(4, "big")
    frame[20:24] = c1.to_bytes(4, "big")

    path = tmp_path / "uncommitted.sqlite-wal"
    path.write_bytes(raw + bytes(frame))
    log2 = parse_wal(str(path))
    assert log2.version_count == log.version_count
    assert log2.uncommitted_frames == 1
    assert "未形成提交" in (log2.note or "")
    report = _report(main_db, str(path), work)
    assert report.total_versions == len(log.commits)
    assert report.first_complete.version == 3


def test_wal_page_size_mismatch_rejected(tmp_path):
    main_db, _wal = create_wal_sample(str(tmp_path / "s"))
    fake = tmp_path / "fake-wal"
    header = bytearray(32)
    header[0:4] = (0x377F0682).to_bytes(4, "big")
    header[4:8] = (3007000).to_bytes(4, "big")
    header[8:12] = (8192).to_bytes(4, "big")
    fake.write_bytes(header)
    with pytest.raises(WalError):
        parse_wal(str(fake), 4096)


def test_not_a_sqlite_main(tmp_path, work):
    bad = tmp_path / "bad.db"
    bad.write_bytes(b"nope")
    with pytest.raises(AnalysisError):
        _report(str(bad), str(bad) + "-wal", work)


def test_missing_analysis_columns(tmp_path, work):
    db = tmp_path / "old.db"
    conn = sqlite3.connect(db)
    conn.execute("CREATE TABLE tests (id INTEGER PRIMARY KEY, name TEXT)")
    conn.execute("CREATE TABLE readings (id INTEGER PRIMARY KEY, test_id INTEGER)")
    conn.commit()
    conn.close()
    with pytest.raises(AnalysisError) as exc:
        _report(str(db), str(db) + "-wal", work)
    assert "level_db" in str(exc.value)


def test_test_absent_from_main_is_allowed(wal_sample, work):
    """试验可能到某笔提交才创建：第 0 版不存在也继续逐版核查。

    样例中没有 #99 的任何读数，因此一直没测齐，最后一版缺全部频点；
    报告会标记该试验不在主库中。
    """
    main_db, wal = wal_sample
    report = _report(main_db, wal, work, test_id=99)
    assert report.test_in_main is False
    assert report.test_name == "试验 #99"
    assert report.total_versions == 5
    assert report.first_complete is None
    assert report.last_state.version == 5
    assert report.last_state.missing == ["100", "125", "160", "200"]


def test_parse_frequencies():
    assert parse_frequencies("100, 125，160 200;200") == [100.0, 125.0, 160.0, 200.0]
    with pytest.raises(ValueError):
        parse_frequencies("abc")
    with pytest.raises(ValueError):
        parse_frequencies("  ")


def test_latest_reading_with_nonempty_level_wins(tmp_path):
    """同一频点有多条读数时取编号最大、且隔声量非空的一条。

    「非空」按需求口径：NULL、空白文本不算已测；非数值文本（如 "n/a"）
    属于「有内容」的隔声量，仍算已测。
    """
    db = tmp_path / "cover.sqlite"
    conn = sqlite3.connect(db)
    conn.executescript(
        """
        CREATE TABLE tests (id INTEGER PRIMARY KEY, name TEXT);
        CREATE TABLE readings (
            id INTEGER PRIMARY KEY, test_id INTEGER,
            frequency_hz REAL, level_db
        );
        INSERT INTO tests VALUES (1, 'cover');
        INSERT INTO readings VALUES (1, 1, 100.0, 50.0);
        INSERT INTO readings VALUES (2, 1, 100.0, 51.0);
        INSERT INTO readings VALUES (3, 1, 100.0, NULL);
        """
    )
    conn.commit()
    conn.close()
    wal = tmp_path / "cover.sqlite-wal"
    wal.write_bytes(b"")  # 空 WAL：只有第 0 版
    report = _report(str(db), str(wal), str(tmp_path / "w.db"), freqs=(100,))
    assert report.first_complete.version == 0
    # 编号最大的非空读数：3 为 NULL，取 2
    assert report.first_complete.readings == {"100": 2}

    conn = sqlite3.connect(db)
    conn.execute("UPDATE readings SET level_db = 'n/a' WHERE id = 3")
    conn.commit()
    conn.close()
    report = _report(str(db), str(wal), str(tmp_path / "w2.db"), freqs=(100,))
    assert report.first_complete.readings == {"100": 3}


def test_test_created_in_a_commit(tmp_path):
    """试验与读数都在第 1 笔提交中才出现：仍能报告第 1 版测齐。"""
    live = str(tmp_path / "live.sqlite")
    conn = sqlite3.connect(live, isolation_level=None)
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA wal_autocheckpoint=0")
        conn.executescript(
            "CREATE TABLE tests(id INTEGER PRIMARY KEY, name TEXT);"
            "CREATE TABLE readings(id INTEGER PRIMARY KEY, test_id INTEGER,"
            " frequency_hz REAL, level_db REAL);"
        )
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        main_db = str(tmp_path / "main.sqlite")
        shutil.copyfile(live, main_db)
        conn.execute("BEGIN")
        conn.execute("INSERT INTO tests VALUES (7, 'late test')")
        conn.execute("INSERT INTO readings VALUES (1, 7, 100.0, 40.0)")
        conn.execute("INSERT INTO readings VALUES (2, 7, 125.0, 41.0)")
        conn.execute("COMMIT")
        wal = str(tmp_path / "upload.wal")
        shutil.copyfile(live + "-wal", wal)
    finally:
        conn.close()
    report = check_versions(
        main_db, wal, str(tmp_path / "work.db"), 7, [100.0, 125.0]
    )
    assert report.test_in_main is False
    assert report.first_complete.version == 1
    assert report.first_complete.readings == {"100": 1, "125": 2}
    assert report.test_name == "late test"


def _reference_rows(wal_sample, tmp_path):
    """参考实现：每版都单独复制主库并累计回放日志，返回各版 readings 行。"""
    main_db, wal = wal_sample
    log = parse_wal(wal)
    page_size = log.page_size
    rows: dict[int, list] = {}
    for commit in log.commits:
        ref = os.path.join(str(tmp_path), f"ref{commit.number}.db")
        shutil.copyfile(main_db, ref)
        with open(ref, "r+b") as f:
            f.seek(18)
            f.write(b"\x01\x01")
        with open(wal, "rb") as src, open(ref, "r+b") as dst:
            for prev in log.commits[: commit.number]:
                for frame in prev.frames:
                    src.seek(frame.data_offset)
                    dst.seek((frame.page_no - 1) * page_size)
                    dst.write(src.read(page_size))
        conn = sqlite3.connect(ref)
        rows[commit.number] = conn.execute(
            "SELECT id, level_db FROM readings ORDER BY id"
        ).fetchall()
        conn.close()
    return rows


def test_incremental_analysis_matches_full_copy(wal_sample, tmp_path, work):
    """逐版结果应与「每版单独复制主库并累计回放」的参考实现一致。"""
    main_db, wal = wal_sample
    # 先量参考实现，再进行分析（分析会打开主库，可能触发一次 checkpoint）
    reference = _reference_rows(wal_sample, tmp_path)

    trace = []
    report = check_versions(
        main_db, wal, work, 1, [100, 125, 160, 200], wal_log=parse_wal(wal), trace=trace
    )
    assert report.first_complete.version == 3
    # 由于在第 3 版即测齐，逐版状态到第 3 版为止
    assert [s.version for s in trace] == [0, 1, 2, 3]

    # 加上 250 Hz（从未测过）→ 走完全部版本，检查撤回与重测
    trace = []
    report = check_versions(
        main_db,
        wal,
        work,
        1,
        [100, 125, 160, 200, 250],
        wal_log=parse_wal(wal),
        trace=trace,
    )
    assert report.first_complete is None
    assert [s.version for s in trace] == [0, 1, 2, 3, 4, 5]
    assert trace[4].missing == ["200", "250"]  # 第 4 版撤回 200 Hz
    assert trace[5].readings["200"] == 105  # 第 5 版重测
    # 抽查参考实现与逐版结论一致
    assert reference[3][-1] == (104, 47.2)
    assert reference[4] == [(101, 52.1), (102, 51.0), (103, 49.8), (104, None)]
    assert reference[5][-1] == (105, 47.0)
