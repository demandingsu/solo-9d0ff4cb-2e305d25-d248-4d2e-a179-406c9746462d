"""按 WAL 提交逐版核查必测频点是否测齐。

背景：一次扫频的读数入库后，后续提交可能补测、覆盖或撤回个别读数。
实验员给出试验与必测频点，需要知道「最早哪一笔提交把所有频点测齐」。

数据来源：
- 采集开始时的 SQLite 主库（第 0 版）；
- 同一轮、尚未回填（checkpoint）的 WAL，其中每一笔完整且校验通过的
  提交算一版（第 1…N 版）。

实现要点：
- WAL 只解析一次，得到每笔提交的帧偏移；校验和/盐值不匹配、文件截断
  或末尾未形成提交的帧一律忽略（即「截断的日志尾部」「未提交的数据」）；
- 分析时把主库复制到工作文件**一次**，随后按提交顺序把该笔提交的页
  原地写入工作文件（累计应用），每版用新的只读连接查询该试验的读数。
  不会为每笔提交重新复制主库，也不会回放此前的提交。
"""
from __future__ import annotations

import array
import bisect
import math
import os
import re
import shutil
import sqlite3
import sys
from dataclasses import dataclass, field
from urllib.parse import quote

WAL_MAGIC = (0x377F0682, 0x377F0683)  # 最低位表示校验和的字节序
WAL_VERSION = 3007000
WAL_HEADER_SIZE = 32
FRAME_HEADER_SIZE = 24

SQLITE_MAGIC = b"SQLite format 3\x00"

# 核查分析所需的最小结构（其余列任意）
ANALYSIS_COLUMNS = {
    "tests": ("id", "name"),
    "readings": ("id", "test_id", "frequency_hz", "level_db"),
}


class WalError(Exception):
    """WAL 文件本身无法解析（区别于可忽略的日志尾部）。"""


class AnalysisError(Exception):
    """分析无法进行（结构缺失、试验不存在等）。"""


@dataclass
class FrameRef:
    """一帧 WAL：只记录页号与页面数据在文件中的偏移，避免整份日志驻留内存。"""

    page_no: int
    data_offset: int


@dataclass
class Commit:
    """一笔有效提交（以校验通过的提交帧结尾）。"""

    number: int  # 1 起的提交序号，即版本序号
    frames: list[FrameRef] = field(default_factory=list)
    db_size: int = 0  # 提交后的数据库页数


@dataclass
class WalLog:
    page_size: int
    checksum_seed: tuple[int, int] = (0, 0)  # 文件头中的校验和（帧链的起点）
    checksum_order: str = "big"
    commits: list[Commit] = field(default_factory=list)
    ignored_bytes: int = 0
    uncommitted_frames: int = 0
    note: str | None = None

    @property
    def version_count(self) -> int:
        return len(self.commits)


def _checksum(data: bytes, s0: int, s1: int, order: str) -> tuple[int, int]:
    """SQLite WAL 校验和：按 8 字节一组，两个 32 位累加器交替叠加。

    用 array 一次性解码成 32 位字，比逐组 int.from_bytes 快约 2.5 倍；
    WAL 页大小总是 8 的倍数，这里的补零只针对文件头。
    """
    if len(data) % 8:
        data += b"\x00" * (8 - len(data) % 8)
    words = array.array("I")
    words.frombytes(data)
    if (sys.byteorder == "big") != (order == "big"):
        words.byteswap()
    it = iter(words)
    for x0 in it:
        s0 = (s0 + x0 + s1) & 0xFFFFFFFF
        x1 = next(it)
        s1 = (s1 + x1 + s0) & 0xFFFFFFFF
    return s0, s1


def main_db_page_size(path: str) -> int:
    """读取主库页大小；不是可识别的 SQLite 文件时抛 AnalysisError。"""
    try:
        with open(path, "rb") as f:
            header = f.read(100)
    except OSError as exc:
        raise AnalysisError(f"主库文件无法读取：{exc}") from exc
    if len(header) < 100 or not header.startswith(SQLITE_MAGIC):
        raise AnalysisError("主库文件不是有效的 SQLite 数据库")
    raw = int.from_bytes(header[16:18], "big")
    page_size = 65536 if raw == 1 else raw
    if page_size < 512 or page_size > 65536 or page_size & (page_size - 1):
        raise AnalysisError(f"主库文件头中的页大小无效：{raw}")
    return page_size


def parse_wal(path: str, main_page_size_hint: int | None = None) -> WalLog:
    """解析 WAL，返回有效提交列表及被忽略的尾部信息。

    可忽略的情况（只记 note，不报错）：帧被截断、盐值不匹配、校验和失败、
    末尾帧未形成提交。WAL 文件头不合法或与主库页大小不一致则抛 WalError。
    空 WAL 表示没有任何提交，只核查第 0 版。
    """
    try:
        size = os.path.getsize(path)
    except OSError as exc:
        raise WalError(f"WAL 文件无法读取：{exc}") from exc

    if size == 0:
        return WalLog(
            page_size=main_page_size_hint or 0,
            note="WAL 为空，仅核查主库第 0 版",
        )

    with open(path, "rb") as f:
        header = f.read(WAL_HEADER_SIZE)
        if len(header) < WAL_HEADER_SIZE:
            raise WalError("WAL 文件头不完整，无法解析")

        magic = int.from_bytes(header[0:4], "big")
        if magic not in WAL_MAGIC:
            raise WalError("不是有效的 SQLite WAL 文件（文件头标识不符）")
        version = int.from_bytes(header[4:8], "big")
        if version != WAL_VERSION:
            raise WalError(f"不支持的 WAL 版本：{version}")

        raw_ps = int.from_bytes(header[8:12], "big")
        page_size = 65536 if raw_ps == 1 else raw_ps
        if (
            page_size < 512
            or page_size > 65536
            or page_size & (page_size - 1)
        ):
            raise WalError(f"WAL 文件头中的页大小无效：{raw_ps}")
        if main_page_size_hint and page_size != main_page_size_hint:
            raise WalError(
                f"WAL 页大小（{page_size}）与主库（{main_page_size_hint}）不一致"
            )

        order = "big" if magic & 1 else "little"
        stored0 = int.from_bytes(header[24:28], "big")
        stored1 = int.from_bytes(header[28:32], "big")
        calc0, calc1 = _checksum(header[:24], 0, 0, order)
        if (calc0, calc1) != (stored0, stored1):
            raise WalError("WAL 文件头校验失败，文件可能已损坏")

        salt1 = header[16:20]
        salt2 = header[20:24]

        log = WalLog(
            page_size=page_size,
            checksum_seed=(stored0, stored1),
            checksum_order=order,
        )
        # 帧校验和是链式的：以文件头中保存的校验和为种子，逐帧累计
        # （不是从 0 开始，否则除第一帧外全部校验失败）
        s0, s1 = stored0, stored1
        offset = WAL_HEADER_SIZE  # 下一帧起点
        last_commit_end = WAL_HEADER_SIZE
        pending: list[FrameRef] = []
        notes: list[str] = []

        while size - offset >= FRAME_HEADER_SIZE:
            f.seek(offset)
            fh = f.read(FRAME_HEADER_SIZE)
            if len(fh) < FRAME_HEADER_SIZE:  # 理论上外层条件已排除
                notes.append(f"日志尾部有截断的帧（{size - offset} 字节），已忽略")
                break

            if fh[8:12] != salt1 or fh[12:16] != salt2:
                notes.append(
                    "检测到盐值不匹配的后续帧（可能属于另一轮日志），其后内容已忽略"
                )
                break
            if size - offset < FRAME_HEADER_SIZE + page_size:
                notes.append(
                    f"日志尾部有截断的帧（{size - offset} 字节），已忽略"
                )
                break

            f.seek(offset + FRAME_HEADER_SIZE)
            data = f.read(page_size)
            if len(data) < page_size:
                notes.append(
                    f"日志尾部有截断的帧（{size - offset} 字节），已忽略"
                )
                break

            fck0 = int.from_bytes(fh[16:20], "big")
            fck1 = int.from_bytes(fh[20:24], "big")
            c0, c1 = _checksum(fh[:8] + data, s0, s1, order)
            if (c0, c1) != (fck0, fck1):
                notes.append(f"日志尾部校验失败的帧（{size - offset} 字节），其后内容已忽略")
                break

            page_no = int.from_bytes(fh[0:4], "big")
            db_size = int.from_bytes(fh[4:8], "big")
            if page_no == 0:
                notes.append(
                    f"日志尾部出现无效页号 0 的帧（{size - offset} 字节），其后内容已忽略"
                )
                break
            pending.append(FrameRef(page_no, offset + FRAME_HEADER_SIZE))
            s0, s1 = c0, c1
            offset += FRAME_HEADER_SIZE + page_size
            if db_size:
                log.commits.append(
                    Commit(len(log.commits) + 1, pending, db_size)
                )
                pending = []
                last_commit_end = offset

        log.uncommitted_frames = len(pending)
        log.ignored_bytes = size - last_commit_end
        if pending and not notes:
            notes.append(f"末尾 {len(pending)} 帧未形成提交，已忽略")
        log.note = "；".join(notes) if notes else None
        return log


def validate_analysis_schema(conn: sqlite3.Connection) -> list[str]:
    """校验核查分析所需的最小表/列，返回错误列表。"""
    errors: list[str] = []
    tables = {
        r[0]
        for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    }
    for table, cols in ANALYSIS_COLUMNS.items():
        if table not in tables:
            errors.append(f"缺少数据表：{table}")
            continue
        have = {r[1] for r in conn.execute(f"PRAGMA table_info({table})")}
        for col in cols:
            if col not in have:
                errors.append(f"表 {table} 缺少列：{col}")
    return errors


def open_readonly(path: str) -> sqlite3.Connection:
    """以只读、无锁方式打开工作库（分析期间工作文件会被原地更新）。"""
    uri = "file:" + quote(os.path.abspath(path)) + "?mode=ro&immutable=1"
    return sqlite3.connect(uri, uri=True)


def _force_rollback_mode(path: str) -> None:
    """把主库文件头的日志模式标为 rollback。

    主库常处于 WAL 模式（文件头第 18/19 字节为 2）；工作副本不附带 -wal，
    若不改写，SQLite 会把它当作 WAL 库并尝试创建/回放日志。工作副本的
    页数据由我们按提交手动应用，因此标记为 rollback 即可。
    """
    with open(path, "r+b") as f:
        header = f.read(20)
        if len(header) < 20 or not header.startswith(SQLITE_MAGIC):
            raise AnalysisError("主库文件不是有效的 SQLite 数据库")
        f.seek(18)
        f.write(b"\x01\x01")


def _apply_commit(
    work_path: str, wal_path: str, commit: Commit, page_size: int
) -> None:
    """把一笔提交的页写入工作副本（只写该提交自己的帧）。"""
    with open(wal_path, "rb") as src, open(work_path, "r+b") as dst:
        for frame in commit.frames:
            src.seek(frame.data_offset)
            data = src.read(page_size)
            dst.seek((frame.page_no - 1) * page_size)
            dst.write(data)
        if commit.db_size:
            dst.truncate(commit.db_size * page_size)
        # 该提交可能改写了第 1 页（页头日志模式），保持工作副本为 rollback 模式
        dst.seek(18)
        dst.write(b"\x01\x01")


def parse_frequencies(text: str) -> list[float]:
    """解析用户输入的必测频点（逗号/中文逗号/顿号/分号/空白分隔）。"""
    tokens = [t for t in re.split(r"[,，、;；\s]+", text.strip()) if t]
    if not tokens:
        raise ValueError("请填写至少一个必测频点")
    values: list[float] = []
    for token in tokens:
        try:
            value = float(token)
        except ValueError as exc:
            raise ValueError(f"无法识别的频点：{token}") from exc
        if not math.isfinite(value) or value <= 0:
            raise ValueError(f"频点必须是正数：{token}")
        if not any(math.isclose(value, v, rel_tol=1e-6, abs_tol=1e-6) for v in values):
            values.append(value)
    return values


def _fmt_freq(value: float) -> str:
    return f"{value:g}"


@dataclass
class VersionState:
    version: int
    readings: dict[str, int]  # 频点（格式化文本）-> 读数编号
    missing: list[str]

    @property
    def complete(self) -> bool:
        return not self.missing


@dataclass
class CheckReport:
    test_id: int
    test_name: str
    frequencies: list[str]
    total_versions: int  # 有效提交笔数 N
    first_complete: VersionState | None
    last_state: VersionState | None  # 一直没测齐时为最后一版的状态
    wal_note: str | None
    ignored_bytes: int
    uncommitted_frames: int
    test_in_main: bool = True  # 第 0 版是否已存在该试验


def _state_at(
    conn: sqlite3.Connection, test_id: int, frequencies: list[float], version: int
) -> VersionState:
    """查询某版本下该试验各频点的读数；同一频点取编号最大的已测读数。

    「已测」按需求口径：该频点存在隔声量非空（非 NULL、非空白文本）的读数。
    聚合在 SQL 内完成（走 test_id 索引），每版只返回各频点一行，避免把
    该试验的全部读数读进 Python。
    """
    candidates = conn.execute(
        "SELECT frequency_hz, MAX(id) FROM readings "
        "WHERE test_id = ? AND level_db IS NOT NULL "
        "AND TRIM(CAST(level_db AS TEXT)) <> '' "
        "GROUP BY frequency_hz",
        (test_id,),
    ).fetchall()

    # 频点按大小排序后用二分查找匹配，避免每个候选频点都与全部必测频点比较
    ordered = sorted(enumerate(frequencies), key=lambda pair: pair[1])
    sorted_values = [value for _, value in ordered]
    best: dict[int, int] = {}
    for freq, rid in candidates:
        if rid is None:
            continue
        try:
            fv = float(freq)
        except (TypeError, ValueError):
            continue
        pos = bisect.bisect_left(sorted_values, fv)
        for probe in (pos - 1, pos):
            if 0 <= probe < len(ordered):
                idx, wanted = ordered[probe]
                if math.isclose(fv, wanted, rel_tol=1e-6, abs_tol=1e-6):
                    if idx not in best or rid > best[idx]:
                        best[idx] = rid
                    break
    readings: dict[str, int] = {}
    missing: list[str] = []
    for idx, wanted in enumerate(frequencies):
        label = _fmt_freq(wanted)
        if idx in best:
            readings[label] = best[idx]
        else:
            missing.append(label)
    return VersionState(version, readings, missing)


def check_versions(
    main_path: str,
    wal_path: str,
    work_path: str,
    test_id: int,
    frequencies: list[float],
    wal_log: WalLog | None = None,
    trace: list[VersionState] | None = None,
) -> CheckReport:
    """逐版核查必测频点，返回首次测齐的版本（找不到则为最后一版的缺口）。

    主库只复制一次到 work_path；之后每笔提交仅应用自己的帧，并重新查询
    该试验读数，不复制主库、不回放此前提交。

    trace 若传入列表，会按版本顺序追加第 0…N 版的状态（用于核对/测试）。
    """
    if not frequencies:
        raise AnalysisError("请填写至少一个必测频点")

    page_size = main_db_page_size(main_path)

    # 主库结构校验（第 0 版）放在解析 WAL 之前，结构问题优先提示
    try:
        conn = open_readonly(main_path)
    except sqlite3.DatabaseError as exc:
        raise AnalysisError(f"主库文件无法读取：{exc}") from exc
    try:
        errors = validate_analysis_schema(conn)
        if errors:
            raise AnalysisError("主库缺少分析所需的结构：" + "；".join(errors))
        row = conn.execute(
            "SELECT name FROM tests WHERE id = ?", (test_id,)
        ).fetchone()
        # 试验可能到某笔提交才创建：主库（第 0 版）里没有时仍继续逐版核查，
        # 只是第 0 版所有频点都算未测。
        test_name = (row[0] if row and row[0] else None) or f"试验 #{test_id}"
        test_in_main = row is not None
        base_state = _state_at(conn, test_id, frequencies, 0)
    finally:
        conn.close()

    log = wal_log if wal_log is not None else parse_wal(wal_path, page_size)
    if log.page_size and log.page_size != page_size:
        raise AnalysisError(
            f"WAL 页大小（{log.page_size}）与主库（{page_size}）不一致"
        )
    # 帧页号必须落在主库页数增长可解释的范围内：提交帧的 db_size 是提交后页数
    for commit in log.commits:
        if not commit.db_size:
            continue
        for frame in commit.frames:
            if frame.page_no > commit.db_size:
                raise AnalysisError(
                    f"WAL 与主库不匹配：第 {commit.number} 笔提交引用了页 "
                    f"{frame.page_no}，超过该提交记录的库大小 {commit.db_size} 页"
                )

    # 工作副本只创建一次（先在临时名写入，避免读到上一次残留的半成品）
    tmp_path = work_path + ".tmp"
    try:
        shutil.copyfile(main_path, tmp_path)
        os.replace(tmp_path, work_path)
    except OSError as exc:
        raise AnalysisError(f"无法创建工作文件：{exc}") from exc
    if trace is not None:
        trace.append(base_state)
    try:
        _force_rollback_mode(work_path)

        if base_state.complete:
            return CheckReport(
                test_id=test_id,
                test_name=test_name,
                frequencies=[_fmt_freq(f) for f in frequencies],
                total_versions=log.version_count,
                first_complete=base_state,
                last_state=None,
                test_in_main=test_in_main,
                wal_note=log.note,
                ignored_bytes=log.ignored_bytes,
                uncommitted_frames=log.uncommitted_frames,
            )

        last_state = base_state
        for commit in log.commits:
            _apply_commit(work_path, wal_path, commit, page_size)
            try:
                conn = open_readonly(work_path)
            except sqlite3.DatabaseError as exc:
                raise AnalysisError(
                    f"应用第 {commit.number} 版后无法读取工作库：{exc}"
                ) from exc
            try:
                state = _state_at(conn, test_id, frequencies, commit.number)
                if not test_in_main and test_name == f"试验 #{test_id}":
                    got = conn.execute(
                        "SELECT name FROM tests WHERE id = ?", (test_id,)
                    ).fetchone()
                    if got and got[0]:
                        test_name = got[0]
            finally:
                conn.close()
            if trace is not None:
                trace.append(state)
            last_state = state
            if state.complete:
                return CheckReport(
                    test_id=test_id,
                    test_name=test_name,
                    frequencies=[_fmt_freq(f) for f in frequencies],
                    total_versions=log.version_count,
                    first_complete=state,
                    last_state=None,
                    test_in_main=test_in_main,
                    wal_note=log.note,
                    ignored_bytes=log.ignored_bytes,
                    uncommitted_frames=log.uncommitted_frames,
                )

        return CheckReport(
            test_id=test_id,
            test_name=test_name,
            frequencies=[_fmt_freq(f) for f in frequencies],
            total_versions=log.version_count,
            first_complete=None,
            last_state=last_state,
            test_in_main=test_in_main,
            wal_note=log.note,
            ignored_bytes=log.ignored_bytes,
            uncommitted_frames=log.uncommitted_frames,
        )
    finally:
        for path in (work_path, work_path + ".tmp"):
            try:
                os.remove(path)
            except OSError:
                pass
