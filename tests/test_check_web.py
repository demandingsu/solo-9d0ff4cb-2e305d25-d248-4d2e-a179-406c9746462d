"""频点测齐核查的 Web 流程测试：上传主库+WAL → 选试验/填频点 → 结果。"""
import os
import sqlite3

import pytest
from fastapi.testclient import TestClient

from app import main
from scripts.make_wal_sample import create_wal_sample


@pytest.fixture()
def client(tmp_path, monkeypatch):
    monkeypatch.setattr(main, "DATA_DIR", str(tmp_path / "data"))
    main.SESSIONS.clear()
    main.CHECK_SESSIONS.clear()
    return TestClient(main.app)


@pytest.fixture()
def wal_sample(tmp_path):
    main_db, wal = create_wal_sample(str(tmp_path / "wal_sample"))
    return main_db, wal


def _upload(client, main_db, wal, **data):
    with open(main_db, "rb") as mf, open(wal, "rb") as wf:
        return client.post(
            "/check/upload",
            files={
                "main_db": ("in_situ.sqlite", mf, "application/x-sqlite3"),
                "wal": ("in_situ.sqlite-wal", wf, "application/octet-stream"),
            },
            data=data,
            follow_redirects=False,
        )


def test_check_entry_linked_from_upload(client):
    resp = client.get("/")
    assert 'href="/check"' in resp.text


def test_full_check_flow(client, wal_sample):
    main_db, wal = wal_sample
    resp = _upload(client, main_db, wal)
    assert resp.status_code == 303
    assert resp.headers["location"] == "/check/form"

    form = client.get("/check/form")
    assert form.status_code == 200
    assert "A墙-计权隔声-第1次" in form.text  # 试验列表来自主库

    resp = client.post(
        "/check/run", data={"test_id": "1", "frequencies": "100,125,160,200"}
    )
    assert resp.status_code == 200
    assert "第 3 版" in resp.text
    assert "101" in resp.text and "104" in resp.text


def test_check_upload_with_inline_params(client, wal_sample):
    main_db, wal = wal_sample
    _upload(client, main_db, wal, test_id="1", frequencies="100 125 160 200")
    resp = client.post(
        "/check/run", data={"test_id": "1", "frequencies": "100 125 160 200"}
    )
    assert "第 3 版" in resp.text


def test_never_complete_lists_missing(client, wal_sample):
    main_db, wal = wal_sample
    _upload(client, main_db, wal)
    resp = client.post(
        "/check/run", data={"test_id": "1", "frequencies": "100,125,160,200,250"}
    )
    assert "仍未测齐" in resp.text
    assert "最后一版缺少的频点" in resp.text
    assert "250" in resp.text


def test_truncated_wal_shown(client, wal_sample):
    main_db, wal = wal_sample
    truncated = wal.replace("-main.sqlite-wal", "-truncated.sqlite-wal")
    _upload(client, main_db, truncated)
    resp = client.post(
        "/check/run", data={"test_id": "1", "frequencies": "100,125,160,200"}
    )
    assert resp.status_code == 200
    assert "截断" in resp.text
    assert "仍未测齐" in resp.text
    assert "160" in resp.text and "200" in resp.text


def test_check_rejects_missing_columns(client, tmp_path, wal_sample):
    _main, wal = wal_sample
    db = tmp_path / "old.db"
    conn = sqlite3.connect(db)
    conn.execute("CREATE TABLE tests (id INTEGER PRIMARY KEY, name TEXT)")
    conn.execute("CREATE TABLE readings (id INTEGER PRIMARY KEY, test_id INTEGER)")
    conn.commit()
    conn.close()
    resp = _upload(client, str(db), wal)
    assert resp.status_code == 400
    assert "level_db" in resp.text


def test_check_rejects_non_sqlite_main(client, tmp_path, wal_sample):
    _main, wal = wal_sample
    bad = tmp_path / "bad.db"
    bad.write_bytes(b"not a database")
    resp = _upload(client, str(bad), wal)
    assert resp.status_code == 400
    assert "SQLite" in resp.text


def test_check_rejects_bad_wal(client, wal_sample, tmp_path):
    main_db, _wal = wal_sample
    bad = tmp_path / "bad.db-wal"
    bad.write_bytes(b"garbage" * 100)
    resp = _upload(client, main_db, str(bad))
    assert resp.status_code == 400
    assert "WAL" in resp.text


def test_check_run_validates_input(client, wal_sample):
    main_db, wal = wal_sample
    _upload(client, main_db, wal)
    resp = client.post("/check/run", data={"test_id": "", "frequencies": "100"})
    assert resp.status_code == 400
    assert "请填写" in resp.text
    resp = client.post("/check/run", data={"test_id": "1", "frequencies": "abc"})
    assert resp.status_code == 400
    assert "无法识别" in resp.text
    resp = client.post("/check/run", data={"test_id": "1", "frequencies": "100"})
    assert resp.status_code == 200


def test_check_pages_require_session(client):
    assert client.get("/check/form", follow_redirects=False).status_code == 303
    assert client.post("/check/run", data={}, follow_redirects=False).status_code == 303


def test_upload_does_not_checkpoint_sources(client, wal_sample):
    """上传/核查不能回填用户的主库：主库仍是第 0 版，WAL 仍在。"""
    main_db, wal = wal_sample
    before_main = open(main_db, "rb").read()
    before_wal = open(wal, "rb").read()
    _upload(client, main_db, wal)
    client.post("/check/run", data={"test_id": "1", "frequencies": "100,125,160,200"})
    assert open(main_db, "rb").read() == before_main
    assert open(wal, "rb").read() == before_wal


def test_stored_main_stays_version_zero(client, wal_sample):
    """保存下来的主库副本不应被 WAL 回填（否则「第 0 版」就不成立）。"""
    main_db, wal = wal_sample
    _upload(client, main_db, wal)
    before = open(main_db, "rb").read()
    client.post("/check/run", data={"test_id": "1", "frequencies": "100,125,160,200"})
    check = next(iter(main.CHECK_SESSIONS.values()))
    # 核查会话里保存的主库字节不变；WAL 也不能被就地截断
    assert open(check.main_path, "rb").read() == before
    assert os.path.getsize(check.wal_path) == os.path.getsize(wal)
    # 再次核查仍从第 0 版起算
    resp = client.post(
        "/check/run", data={"test_id": "1", "frequencies": "100,125,160,200"}
    )
    assert "第 3 版" in resp.text


def test_original_upload_still_works(client, sample_db):
    """新增入口不影响原有单文件上传流程。"""
    with open(sample_db, "rb") as f:
        resp = client.post(
            "/upload",
            files={"file": ("lab.db", f, "application/x-sqlite3")},
            follow_redirects=False,
        )
    assert resp.status_code == 303
    client.post("/select", data={"test": ["1"]}, follow_redirects=False)
    assert client.get("/preview").status_code == 200
    assert client.post("/download").status_code == 200
