"""验证逐版核查的增量特性：主库只复制一次，且每版只应用自己的帧。"""
import shutil

from app import versions
from app.versions import check_versions, parse_wal
from scripts.make_wal_sample import create_wal_sample


def test_main_copied_once_and_each_commit_applied_once(tmp_path, monkeypatch):
    main_db, wal = create_wal_sample(str(tmp_path / "s"))
    log = parse_wal(wal)
    assert log.version_count == 5

    copies: list[tuple] = []
    real_copyfile = shutil.copyfile

    def counting_copy(src, dst, *a, **kw):
        copies.append((src, dst))
        return real_copyfile(src, dst, *a, **kw)

    monkeypatch.setattr(versions.shutil, "copyfile", counting_copy)

    applied: list[int] = []
    real_apply = versions._apply_commit

    def counting_apply(work_path, wal_path, commit, page_size):
        applied.append(commit.number)
        return real_apply(work_path, wal_path, commit, page_size)

    monkeypatch.setattr(versions, "_apply_commit", counting_apply)

    work = str(tmp_path / "work.db")
    report = check_versions(main_db, wal, work, 1, [100, 125, 160, 200])

    assert report.first_complete.version == 3
    # 主库只被复制一次（工作副本），不会为每笔提交重新复制
    assert len(copies) == 1
    assert copies[0][0] == main_db
    # 每笔提交只应用一次，且按顺序、不重复回放
    assert applied == [1, 2, 3]


def test_all_versions_processed_when_never_complete(tmp_path, monkeypatch):
    main_db, wal = create_wal_sample(str(tmp_path / "s"))
    applied: list[int] = []
    real_apply = versions._apply_commit

    def counting_apply(work_path, wal_path, commit, page_size):
        applied.append(commit.number)
        return real_apply(work_path, wal_path, commit, page_size)

    monkeypatch.setattr(versions, "_apply_commit", counting_apply)
    report = check_versions(
        main_db, wal, str(tmp_path / "work.db"), 1, [100, 125, 160, 200, 250]
    )
    assert report.first_complete is None
    assert applied == [1, 2, 3, 4, 5]
