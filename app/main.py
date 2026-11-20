"""隔声试验数据交付复核 Web 应用（FastAPI）。

交付流程：上传 SQLite 库 → 勾选试验 / 剔除异常读数 → 预览引用解析结果
（含跨组拉入链与冲突）→ 确认后下载 SQLite 子库。

另有「频点测齐核查」入口：上传采集开始时的主库与同轮 WAL，按有效提交
逐版核查某试验的必测频点最早何时测齐（逻辑见 app/versions.py）。
"""
from __future__ import annotations

import os
import shutil
import sqlite3
import time
import uuid
from dataclasses import dataclass, field

from fastapi import FastAPI, Form, Request, UploadFile
from fastapi.responses import FileResponse, HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from .core import build_subset, load_model, resolve, validate_schema
from .versions import (
    AnalysisError,
    WalError,
    check_versions,
    open_readonly,
    parse_frequencies,
    parse_wal,
    validate_analysis_schema,
)

DATA_DIR = os.environ.get("DATA_DIR", "./data")
MAX_UPLOAD_MB = int(os.environ.get("MAX_UPLOAD_MB", "200"))

app = FastAPI(title="隔声试验数据交付复核")
templates = Jinja2Templates(
    directory=os.path.join(os.path.dirname(os.path.abspath(__file__)), "templates")
)


@dataclass
class SessionState:
    workdir: str
    source_name: str
    selected_tests: set[int] = field(default_factory=set)
    excluded_readings: set[int] = field(default_factory=set)

    @property
    def db_path(self) -> str:
        return os.path.join(self.workdir, "source.db")


@dataclass
class CheckState:
    """频点核查会话：主库、WAL 与上次分析的输入（便于重试/换参数）。"""

    workdir: str
    main_name: str
    wal_name: str
    main_path: str
    wal_path: str
    test_id: int
    frequencies_text: str


# 单进程内存会话；重启即失效（上传文件保留在 DATA_DIR 下，由运维清理）
SESSIONS: dict[str, SessionState] = {}
CHECK_SESSIONS: dict[str, CheckState] = {}


def _get_session(request: Request) -> SessionState | None:
    sid = request.cookies.get("sid")
    return SESSIONS.get(sid) if sid else None


def _get_check_session(request: Request) -> CheckState | None:
    sid = request.cookies.get("csid")
    return CHECK_SESSIONS.get(sid) if sid else None


def _load(st: SessionState):
    conn = sqlite3.connect(f"file:{st.db_path}?mode=ro", uri=True)
    try:
        return load_model(conn)
    finally:
        conn.close()


@app.get("/", response_class=HTMLResponse)
def index(request: Request):
    return templates.TemplateResponse(
        request, "upload.html", {"error": None}
    )


@app.post("/upload", response_class=HTMLResponse)
async def upload(request: Request, file: UploadFile):
    error: str | None = None
    if not file.filename:
        error = "未选择文件"
    data = await file.read()
    if error is None and len(data) > MAX_UPLOAD_MB * 1024 * 1024:
        error = f"文件超过大小限制（{MAX_UPLOAD_MB} MB）"

    if error is None:
        # 旧会话的工作目录一并清理
        old = _get_session(request)
        if old:
            SESSIONS.pop(request.cookies.get("sid"), None)
            shutil.rmtree(old.workdir, ignore_errors=True)

        sid = uuid.uuid4().hex
        workdir = os.path.join(DATA_DIR, sid)
        os.makedirs(workdir, exist_ok=True)
        path = os.path.join(workdir, "source.db")
        with open(path, "wb") as f:
            f.write(data)
        try:
            conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
            errors = validate_schema(conn)
            conn.close()
        except sqlite3.DatabaseError:
            errors = ["文件不是有效的 SQLite 数据库"]
        if errors:
            shutil.rmtree(workdir, ignore_errors=True)
            return templates.TemplateResponse(
                request,
                "upload.html",
                {"error": "；".join(errors)},
                status_code=400,
            )
        SESSIONS[sid] = SessionState(workdir, file.filename or "source.db")
        resp = RedirectResponse("/select", status_code=303)
        resp.set_cookie("sid", sid, httponly=True)
        return resp

    return templates.TemplateResponse(
        request, "upload.html", {"error": error}, status_code=400
    )


# 读数表展示时的优先列序，其余列按出现顺序排后
_PREFERRED_READING_COLS = [
    "id", "kind", "frequency_hz", "level_db",
    "control_reading_id", "calibration_id", "note",
]


@app.get("/select", response_class=HTMLResponse)
def select_page(request: Request):
    st = _get_session(request)
    if not st:
        return RedirectResponse("/", status_code=303)
    model = _load(st)

    headers: list[str] = []
    for r in model.readings.values():
        for col in r:
            if col not in headers:
                headers.append(col)
    headers = [c for c in _PREFERRED_READING_COLS if c in headers] + [
        c for c in headers if c not in _PREFERRED_READING_COLS
    ]

    groups = []
    for gid in sorted(model.groups):
        g = model.groups[gid]
        tests = []
        for t in sorted(model.tests.values(), key=lambda x: x["id"]):
            if t["group_id"] != gid:
                continue
            readings = [
                {
                    "id": r["id"],
                    "cells": [r.get(c) for c in headers],
                    "excluded": r["id"] in st.excluded_readings,
                }
                for r in (
                    model.readings[rid]
                    for rid in model.readings_by_test.get(t["id"], [])
                )
            ]
            tests.append(
                {"row": t, "readings": readings, "selected": t["id"] in st.selected_tests}
            )
        groups.append({"row": g, "tests": tests})

    return templates.TemplateResponse(
        request,
        "select.html",
        {
            "groups": groups,
            "headers": headers,
            "source": st.source_name,
        },
    )


@app.post("/select")
async def select_save(request: Request):
    st = _get_session(request)
    if not st:
        return RedirectResponse("/", status_code=303)
    form = await request.form()
    model = _load(st)

    selected = {int(v) for v in form.getlist("test")} & set(model.tests)
    # 剔除仅对所选试验内的读数生效，其余静默忽略
    excluded = {
        int(v)
        for v in form.getlist("excl")
        if (r := model.readings.get(int(v))) and r["test_id"] in selected
    }
    st.selected_tests = selected
    st.excluded_readings = excluded
    return RedirectResponse("/preview", status_code=303)


@app.get("/preview", response_class=HTMLResponse)
def preview(request: Request):
    st = _get_session(request)
    if not st:
        return RedirectResponse("/", status_code=303)
    model = _load(st)
    res = resolve(model, st.selected_tests, st.excluded_readings)
    return templates.TemplateResponse(
        request,
        "preview.html",
        {
            "res": res,
            "model": model,
            "selected": sorted(st.selected_tests),
            "excluded": sorted(st.excluded_readings),
            "pulled_in": sorted(res.pulled_in.items()),
            "source": st.source_name,
        },
    )


@app.post("/download")
def download(request: Request):
    st = _get_session(request)
    if not st:
        return RedirectResponse("/", status_code=303)
    model = _load(st)
    # 下载前重新解析，防止预览之后状态被修改导致不一致
    res = resolve(model, st.selected_tests, st.excluded_readings)
    if not res.ok:
        return HTMLResponse(
            "<h2>已阻止下载</h2><p>存在未解决的引用冲突（悬空或指向已剔除读数）。"
            "请返回 <a href='/preview'>预览</a> 查看引用链，"
            "或回 <a href='/select'>选择页</a> 修改。</p>",
            status_code=409,
        )
    out_path = os.path.join(st.workdir, f"subset_{int(time.time() * 1000)}.db")
    build_subset(st.db_path, out_path, res)
    name = os.path.splitext(st.source_name)[0] + "_subset.db"
    return FileResponse(
        out_path, filename=name, media_type="application/x-sqlite3"
    )


# ---------------------------------------------------------------- 频点测齐核查


def _render_check(
    request: Request,
    check: CheckState,
    error: str | None = None,
    report=None,
    status_code: int = 200,
):
    # 试验列表以主库为准；若试验只在 WAL 的某笔提交中创建，用户仍可手填编号
    tests = []
    try:
        conn = open_readonly(check.main_path)
        try:
            tests = [
                {"id": r[0], "name": r[1]}
                for r in conn.execute("SELECT id, name FROM tests ORDER BY id")
            ]
        finally:
            conn.close()
    except (sqlite3.DatabaseError, OSError):
        pass
    return templates.TemplateResponse(
        request,
        "check.html",
        {
            "error": error,
            "report": report,
            "tests": tests,
            "main_name": check.main_name,
            "wal_name": check.wal_name,
            "frequencies_text": check.frequencies_text,
            "test_id": check.test_id,
        },
        status_code=status_code,
    )


@app.get("/check", response_class=HTMLResponse)
def check_page(request: Request):
    # 始终展示上传页；已建立的会话可从 /check/form 继续
    return templates.TemplateResponse(
        request, "check_upload.html", {"error": None}
    )


@app.post("/check/upload", response_class=HTMLResponse)
async def check_upload(
    request: Request,
    main_db: UploadFile,
    wal: UploadFile,
    test_id: str = Form(""),
    frequencies: str = Form(""),
):
    """上传主库 + WAL 后建立核查会话；同一表单可顺带填试验与频点。"""
    old = _get_check_session(request)
    if old:
        CHECK_SESSIONS.pop(request.cookies.get("csid"), None)
        shutil.rmtree(old.workdir, ignore_errors=True)

    if not main_db.filename:
        return templates.TemplateResponse(
            request,
            "check_upload.html",
            {"error": "请选择主库文件"},
            status_code=400,
        )
    if not wal.filename:
        return templates.TemplateResponse(
            request,
            "check_upload.html",
            {"error": "请选择同轮、尚未回填的 WAL 文件"},
            status_code=400,
        )

    sid = uuid.uuid4().hex
    workdir = os.path.join(DATA_DIR, sid + "-check")
    os.makedirs(workdir, exist_ok=True)
    # 不以 <main>-wal 命名 WAL：若同名，SQLite 打开主库时会自动回填 WAL，
    # 「主库 = 第 0 版」的前提就被破坏了。分析过程只按物理偏移读取 WAL。
    main_path = os.path.join(workdir, "main.sqlite")
    wal_path = os.path.join(workdir, "upload.wal")

    errors: list[str] = []
    for upload, path in ((main_db, main_path), (wal, wal_path)):
        limit = MAX_UPLOAD_MB * 1024 * 1024
        size = 0
        with open(path, "wb") as f:
            while chunk := await upload.read(1024 * 1024):
                size += len(chunk)
                if size > limit:
                    errors.append(f"文件超过大小限制（{MAX_UPLOAD_MB} MB）")
                    break
                f.write(chunk)
        if errors:
            break
    if not errors:
        try:
            # 只读 + immutable：主库处于 WAL 模式时，普通只读打开会创建
            # -wal/-shm 并可能回填，这里绝不能改动用户上传的主库
            info = open_readonly(main_path)
            try:
                errors = validate_analysis_schema(info)
            finally:
                info.close()
        except (sqlite3.DatabaseError, OSError):
            errors = ["主库不是有效的 SQLite 数据库"]
    if not errors:
        try:
            parse_wal(wal_path)
        except WalError as exc:
            errors = [str(exc)]

    if errors:
        shutil.rmtree(workdir, ignore_errors=True)
        return templates.TemplateResponse(
            request,
            "check_upload.html",
            {"error": "；".join(errors)},
            status_code=400,
        )

    check = CheckState(
        workdir=workdir,
        main_name=main_db.filename or "main.sqlite",
        wal_name=wal.filename or "upload.wal",
        main_path=main_path,
        wal_path=wal_path,
        test_id=int(test_id) if test_id.strip().isdigit() else 0,
        frequencies_text=frequencies.strip(),
    )
    CHECK_SESSIONS[sid] = check
    resp = RedirectResponse("/check/form", status_code=303)
    resp.set_cookie("csid", sid, httponly=True)
    return resp


@app.get("/check/form", response_class=HTMLResponse)
def check_form(request: Request):
    check = _get_check_session(request)
    if not check:
        return RedirectResponse("/check", status_code=303)
    return _render_check(request, check)


@app.post("/check/run", response_class=HTMLResponse)
def check_run(
    request: Request,
    test_id: str = Form(""),
    frequencies: str = Form(""),
):
    check = _get_check_session(request)
    if not check:
        return RedirectResponse("/check", status_code=303)
    check.frequencies_text = frequencies.strip()
    check.test_id = int(test_id) if test_id.strip().isdigit() else 0

    try:
        freq_values = parse_frequencies(frequencies)
    except ValueError as exc:
        return _render_check(request, check, error=str(exc), status_code=400)
    if not check.test_id:
        return _render_check(request, check, error="请填写要核查的试验编号", status_code=400)

    work_path = os.path.join(check.workdir, "work.db")
    try:
        report = check_versions(
            check.main_path,
            check.wal_path,
            work_path,
            check.test_id,
            freq_values,
        )
    except (AnalysisError, WalError) as exc:
        return _render_check(request, check, error=str(exc), status_code=400)
    except (sqlite3.DatabaseError, OSError) as exc:
        return _render_check(
            request,
            check,
            error=f"读取文件失败：{exc}",
            status_code=400,
        )
    finally:
        # 异常退出时也清掉工作副本（正常路径由 check_versions 清理）
        try:
            os.remove(work_path)
            os.remove(work_path + ".tmp")
        except OSError:
            pass
    return _render_check(request, check, report=report)
