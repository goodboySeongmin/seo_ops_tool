from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


@dataclass(frozen=True)
class DBConfig:
    db_path: Path


def default_db_path(app_root: Path) -> Path:
    # Windows/로컬에서도 안전하게 app/data 아래에 생성
    data_dir = app_root / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    return data_dir / "seo_tool.sqlite3"


def _connect(cfg: DBConfig) -> sqlite3.Connection:
    conn = sqlite3.connect(str(cfg.db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def _table_cols(conn: sqlite3.Connection, table: str) -> List[str]:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return [r["name"] for r in rows]


def _ensure_columns(conn: sqlite3.Connection, table: str, columns: Dict[str, str]) -> None:
    existing = set(_table_cols(conn, table))
    for col, ddl in columns.items():
        if col in existing:
            continue
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {ddl}")


def _ensure_index(conn: sqlite3.Connection, *, name: str, ddl: str) -> None:
    conn.execute(f"CREATE INDEX IF NOT EXISTS {name} ON {ddl}")


def init_db(cfg: DBConfig) -> None:
    """테이블 생성 + 간단 마이그레이션(기존 DB 깨지지 않게)."""
    conn = _connect(cfg)
    try:
        # ---- runs ----
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS runs (
              run_id INTEGER PRIMARY KEY,
              stage TEXT NOT NULL DEFAULT 'DRAFT',

              meta_title TEXT NOT NULL DEFAULT '',
              meta_description TEXT NOT NULL DEFAULT '',
              landing_text TEXT NOT NULL DEFAULT '',

              primary_keyword TEXT NOT NULL DEFAULT '',
              supporting_keywords_json TEXT NOT NULL DEFAULT '[]',
              intent TEXT NOT NULL DEFAULT '구매형',

              h1 TEXT NOT NULL DEFAULT '',
              body_html TEXT NOT NULL DEFAULT '',
              cta TEXT NOT NULL DEFAULT '',
              faq_json TEXT NOT NULL DEFAULT '[]',

              canonical_url TEXT NOT NULL DEFAULT '',
              og_title TEXT NOT NULL DEFAULT '',
              og_description TEXT NOT NULL DEFAULT '',

              -- ✅ 랜딩 전환용 (추가)
              buy_url TEXT NOT NULL DEFAULT '',
              products_json TEXT NOT NULL DEFAULT '[]',

              optimize_json TEXT,
              qc_json TEXT,
              approved_json TEXT,
              audit_json TEXT,
              fixed_json TEXT,
              fix_diff_json TEXT,
              export_json TEXT,

              created_at TEXT NOT NULL,
              updated_at TEXT NOT NULL
            )
            """
        )

        # 기존 DB에 컬럼 없으면 안전하게 추가
        _ensure_columns(
            conn,
            "runs",
            {
                "stage": "TEXT NOT NULL DEFAULT 'DRAFT'",
                "meta_title": "TEXT NOT NULL DEFAULT ''",
                "meta_description": "TEXT NOT NULL DEFAULT ''",
                "landing_text": "TEXT NOT NULL DEFAULT ''",
                "primary_keyword": "TEXT NOT NULL DEFAULT ''",
                "supporting_keywords_json": "TEXT NOT NULL DEFAULT '[]'",
                "intent": "TEXT NOT NULL DEFAULT '구매형'",
                "h1": "TEXT NOT NULL DEFAULT ''",
                "body_html": "TEXT NOT NULL DEFAULT ''",
                "cta": "TEXT NOT NULL DEFAULT ''",
                "faq_json": "TEXT NOT NULL DEFAULT '[]'",
                "canonical_url": "TEXT NOT NULL DEFAULT ''",
                "og_title": "TEXT NOT NULL DEFAULT ''",
                "og_description": "TEXT NOT NULL DEFAULT ''",
                # ✅ 추가 컬럼
                "buy_url": "TEXT NOT NULL DEFAULT ''",
                "products_json": "TEXT NOT NULL DEFAULT '[]'",
                "optimize_json": "TEXT",
                "qc_json": "TEXT",
                "approved_json": "TEXT",
                "audit_json": "TEXT",
                "fixed_json": "TEXT",
                "fix_diff_json": "TEXT",
                "export_json": "TEXT",
                "created_at": "TEXT NOT NULL DEFAULT ''",
                "updated_at": "TEXT NOT NULL DEFAULT ''",
            },
        )

        # ---- events ----
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS events (
              event_id INTEGER PRIMARY KEY AUTOINCREMENT,
              run_id INTEGER NOT NULL,
              variant TEXT NOT NULL,
              event_name TEXT NOT NULL,
              ts TEXT NOT NULL,
              FOREIGN KEY(run_id) REFERENCES runs(run_id) ON DELETE CASCADE
            )
            """
        )

        # ---- job_logs ---- (운영 타임라인)
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS job_logs (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              run_id INTEGER NOT NULL,
              job_name TEXT NOT NULL,
              status TEXT NOT NULL,
              detail_json TEXT NOT NULL DEFAULT '{}',
              elapsed_ms INTEGER NOT NULL DEFAULT 0,
              ts TEXT NOT NULL,
              FOREIGN KEY(run_id) REFERENCES runs(run_id) ON DELETE CASCADE
            )
            """
        )

        # indexes
        _ensure_index(conn, name="idx_runs_updated_at", ddl="runs(updated_at)")
        _ensure_index(conn, name="idx_runs_stage", ddl="runs(stage)")
        _ensure_index(conn, name="idx_events_run_id", ddl="events(run_id)")
        _ensure_index(conn, name="idx_events_run_variant", ddl="events(run_id, variant)")
        _ensure_index(conn, name="idx_job_logs_run_id", ddl="job_logs(run_id)")
        _ensure_index(conn, name="idx_job_logs_ts", ddl="job_logs(ts)")

        # created_at/updated_at 보정
        conn.execute(
            "UPDATE runs SET created_at = COALESCE(NULLIF(created_at,''), ?) WHERE created_at IS NULL OR created_at = ''",
            (_now(),),
        )
        conn.execute(
            "UPDATE runs SET updated_at = COALESCE(NULLIF(updated_at,''), created_at, ?) WHERE updated_at IS NULL OR updated_at = ''",
            (_now(),),
        )

        conn.commit()
    finally:
        conn.close()


def _json_dump(v: Any) -> str:
    return json.dumps(v, ensure_ascii=False)


def _json_load(s: Optional[str], default: Any) -> Any:
    if not s:
        return default
    try:
        return json.loads(s)
    except Exception:
        return default


def _run_row_to_dict(r: sqlite3.Row) -> Dict[str, Any]:
    return {
        "run_id": int(r["run_id"]),
        "stage": r["stage"],
        "created_at": r["created_at"],
        "updated_at": r["updated_at"],
        "meta_title": r["meta_title"],
        "meta_description": r["meta_description"],
        "landing_text": r["landing_text"],
        "primary_keyword": r["primary_keyword"],
        "supporting_keywords": _json_load(r["supporting_keywords_json"], []),
        "intent": r["intent"],
        "h1": r["h1"],
        "body_html": r["body_html"],
        "cta": r["cta"],
        "faq": _json_load(r["faq_json"], []),
        "canonical_url": r["canonical_url"],
        "og_title": r["og_title"],
        "og_description": r["og_description"],
        # ✅ 추가
        "buy_url": r["buy_url"] if "buy_url" in r.keys() else "",
        "products": _json_load(r["products_json"], []) if "products_json" in r.keys() else [],
        "optimize": _json_load(r["optimize_json"], None),
        "qc": _json_load(r["qc_json"], None),
        "approved": _json_load(r["approved_json"], None),
        "audit": _json_load(r["audit_json"], None),
        "fixed": _json_load(r["fixed_json"], None),
        "fix_diff": _json_load(r["fix_diff_json"], None) if ("fix_diff_json" in r.keys()) else None,
        "export": _json_load(r["export_json"], None),
    }


# ------------------------------
# runs
# ------------------------------
def create_run(
    cfg: DBConfig,
    *,
    meta_title: str,
    meta_description: str,
    landing_text: str,
    primary_keyword: str,
    supporting_keywords: List[str],
    intent: str,
    canonical_url: str,
    cta: str,
    buy_url: str = "",
    products: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    conn = _connect(cfg)
    try:
        now = _now()
        body_html = landing_text or ""  # 초기에는 landing_text를 그대로 바디로
        products = products or []

        conn.execute(
            """
            INSERT INTO runs (
              stage,
              meta_title, meta_description, landing_text,
              primary_keyword, supporting_keywords_json, intent,
              h1, body_html, cta, faq_json,
              canonical_url, og_title, og_description,
              buy_url, products_json,
              optimize_json, qc_json, approved_json, audit_json, fixed_json, fix_diff_json, export_json,
              created_at, updated_at
            ) VALUES (
              'DRAFT',
              ?, ?, ?,
              ?, ?, ?,
              '', ?, ?, '[]',
              ?, '', '',
              ?, ?,
              NULL, NULL, NULL, NULL, NULL, NULL, NULL,
              ?, ?
            )
            """,
            (
                meta_title or "",
                meta_description or "",
                landing_text or "",
                (primary_keyword or "").strip(),
                _json_dump([s.strip() for s in supporting_keywords if s and s.strip()]),
                (intent or "구매형").strip(),
                body_html,
                (cta or "").strip(),
                (canonical_url or "").strip(),
                (buy_url or "").strip(),
                _json_dump(products),
                now,
                now,
            ),
        )
        run_id = int(conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"])
        conn.commit()
        return get_run(cfg, run_id)
    finally:
        conn.close()


def get_run(cfg: DBConfig, run_id: int) -> Dict[str, Any]:
    conn = _connect(cfg)
    try:
        r = conn.execute("SELECT * FROM runs WHERE run_id=?", (run_id,)).fetchone()
        if not r:
            raise KeyError(f"run_id {run_id} not found")
        return _run_row_to_dict(r)
    finally:
        conn.close()


def update_run(cfg: DBConfig, run_id: int, *, fields: Dict[str, Any]) -> Dict[str, Any]:
    if not fields:
        return get_run(cfg, run_id)

    json_fields = {
        "supporting_keywords": "supporting_keywords_json",
        "faq": "faq_json",
        "products": "products_json",
        "optimize": "optimize_json",
        "qc": "qc_json",
        "approved": "approved_json",
        "audit": "audit_json",
        "fixed": "fixed_json",
        "fix_diff": "fix_diff_json",
        "export": "export_json",
    }
    direct_fields = {
        "stage": "stage",
        "meta_title": "meta_title",
        "meta_description": "meta_description",
        "landing_text": "landing_text",
        "primary_keyword": "primary_keyword",
        "intent": "intent",
        "h1": "h1",
        "body_html": "body_html",
        "cta": "cta",
        "canonical_url": "canonical_url",
        "og_title": "og_title",
        "og_description": "og_description",
        "buy_url": "buy_url",
    }

    sets: List[str] = []
    params: List[Any] = []

    for k, v in fields.items():
        if k in json_fields:
            sets.append(f"{json_fields[k]}=?")
            params.append(_json_dump(v))
        elif k in direct_fields:
            sets.append(f"{direct_fields[k]}=?")
            params.append(v if v is not None else "")

    sets.append("updated_at=?")
    params.append(_now())
    params.append(run_id)

    if not sets:
        return get_run(cfg, run_id)

    conn = _connect(cfg)
    try:
        conn.execute(f"UPDATE runs SET {', '.join(sets)} WHERE run_id=?", tuple(params))
        conn.commit()
        return get_run(cfg, run_id)
    finally:
        conn.close()


def list_runs(cfg: DBConfig, *, limit: int = 50, offset: int = 0) -> List[Dict[str, Any]]:
    conn = _connect(cfg)
    try:
        rows = conn.execute(
            "SELECT * FROM runs ORDER BY run_id DESC LIMIT ? OFFSET ?",
            (max(1, min(int(limit or 50), 200)), max(0, int(offset or 0))),
        ).fetchall()
        return [_run_row_to_dict(r) for r in rows]
    finally:
        conn.close()


def _build_run_search_where(q: str) -> Tuple[str, List[Any]]:
    q = (q or "").strip()
    if not q:
        return "", []
    try:
        run_id = int(q)
        return "WHERE run_id = ?", [run_id]
    except Exception:
        pass
    like = f"%{q}%"
    return (
        "WHERE primary_keyword LIKE ? OR meta_title LIKE ? OR intent LIKE ? OR meta_description LIKE ?",
        [like, like, like, like],
    )


def list_runs_v2(cfg: DBConfig, *, q: str = "", stage: str = "", limit: int = 50, offset: int = 0) -> Dict[str, Any]:
    """Admin Runs 테이블용 summary (CTR/Audit/Approved/Export 포함)."""
    conn = _connect(cfg)
    try:
        where_sql, params = _build_run_search_where(q)

        stage = (stage or "").strip()
        if stage:
            if where_sql:
                where_sql += " AND r.stage = ?"
            else:
                where_sql = "WHERE r.stage = ?"
            params.append(stage)

        total = int(conn.execute(f"SELECT COUNT(*) AS c FROM runs r {where_sql}", tuple(params)).fetchone()["c"])
        limit_i = max(1, min(int(limit or 50), 200))
        offset_i = max(0, int(offset or 0))

        rows = conn.execute(
            f"""
            SELECT
              r.run_id,
              r.stage,
              r.primary_keyword,
              r.intent,
              r.updated_at,
              r.audit_json,
              r.approved_json,
              r.export_json,

              (SELECT COUNT(*) FROM events e WHERE e.run_id=r.run_id AND e.variant='A' AND e.event_name='view') AS A_view,
              (SELECT COUNT(*) FROM events e WHERE e.run_id=r.run_id AND e.variant='A' AND e.event_name='cta_click') AS A_click,
              (SELECT COUNT(*) FROM events e WHERE e.run_id=r.run_id AND e.variant='B' AND e.event_name='view') AS B_view,
              (SELECT COUNT(*) FROM events e WHERE e.run_id=r.run_id AND e.variant='B' AND e.event_name='cta_click') AS B_click

            FROM runs r
            {where_sql}
            ORDER BY r.run_id DESC
            LIMIT ? OFFSET ?
            """,
            tuple(params + [limit_i, offset_i]),
        ).fetchall()

        items: List[Dict[str, Any]] = []
        for r in rows:
            audit = _json_load(r["audit_json"], None)
            approved = _json_load(r["approved_json"], None)
            export = _json_load(r["export_json"], None)

            a_view = int(r["A_view"] or 0)
            a_click = int(r["A_click"] or 0)
            b_view = int(r["B_view"] or 0)
            b_click = int(r["B_click"] or 0)

            def _ctr(click: int, view: int) -> float:
                return (float(click) / float(view)) if view > 0 else 0.0

            items.append(
                {
                    "run_id": int(r["run_id"]),
                    "stage": r["stage"],
                    "primary_keyword": r["primary_keyword"],
                    "intent": r["intent"],
                    "updated_at": r["updated_at"],
                    "audit_overall": (audit or {}).get("overall") if isinstance(audit, dict) else None,
                    "audit_score": (audit or {}).get("score") if isinstance(audit, dict) else None,
                    "approved": (approved or {}).get("variant") if isinstance(approved, dict) else None,
                    "exported": bool(export),
                    "A": {"view": a_view, "click": a_click, "ctr": _ctr(a_click, a_view)},
                    "B": {"view": b_view, "click": b_click, "ctr": _ctr(b_click, b_view)},
                }
            )

        return {"total": total, "limit": limit_i, "offset": offset_i, "items": items}
    finally:
        conn.close()


def reset_events(cfg: DBConfig, *, run_id: int) -> int:
    conn = _connect(cfg)
    try:
        cur = conn.execute("DELETE FROM events WHERE run_id=?", (run_id,))
        conn.commit()
        return int(cur.rowcount or 0)
    finally:
        conn.close()


def delete_run(cfg: DBConfig, *, run_id: int) -> Dict[str, Any]:
    run = get_run(cfg, run_id)
    conn = _connect(cfg)
    try:
        cur = conn.execute("DELETE FROM runs WHERE run_id=?", (run_id,))
        conn.commit()
        return {"deleted": int(cur.rowcount or 0), "run": run}
    finally:
        conn.close()


# ------------------------------
# events
# ------------------------------
def add_event(cfg: DBConfig, *, run_id: int, variant: str, event_name: str) -> Dict[str, Any]:
    conn = _connect(cfg)
    try:
        ts = _now()
        conn.execute(
            "INSERT INTO events(run_id, variant, event_name, ts) VALUES(?,?,?,?)",
            (run_id, variant, event_name, ts),
        )
        event_id = int(conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"])
        conn.commit()
        return {"event_id": event_id, "run_id": run_id, "variant": variant, "event_name": event_name, "ts": ts}
    finally:
        conn.close()


def list_events(cfg: DBConfig, run_id: int) -> List[Dict[str, Any]]:
    conn = _connect(cfg)
    try:
        rows = conn.execute(
            "SELECT event_id, run_id, variant, event_name, ts FROM events WHERE run_id=? ORDER BY event_id ASC",
            (run_id,),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def list_events_v2(cfg: DBConfig, *, run_id: int, limit: int = 200) -> List[Dict[str, Any]]:
    conn = _connect(cfg)
    try:
        rows = conn.execute(
            "SELECT event_id, run_id, variant, event_name, ts FROM events WHERE run_id=? ORDER BY event_id DESC LIMIT ?",
            (run_id, max(1, min(int(limit or 200), 500))),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


# ------------------------------
# job logs
# ------------------------------
def add_job_log(
    cfg: DBConfig,
    *,
    run_id: int,
    job_name: str,
    status: str,
    detail: Dict[str, Any],
    elapsed_ms: int,
) -> Dict[str, Any]:
    conn = _connect(cfg)
    try:
        ts = _now()
        conn.execute(
            "INSERT INTO job_logs(run_id, job_name, status, detail_json, elapsed_ms, ts) VALUES(?,?,?,?,?,?)",
            (run_id, job_name, status, _json_dump(detail or {}), int(elapsed_ms or 0), ts),
        )
        log_id = int(conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"])
        conn.commit()
        return {
            "id": log_id,
            "run_id": run_id,
            "job_name": job_name,
            "status": status,
            "detail": detail or {},
            "elapsed_ms": int(elapsed_ms or 0),
            "ts": ts,
        }
    finally:
        conn.close()


def list_job_logs(cfg: DBConfig, *, run_id: int, limit: int = 50) -> List[Dict[str, Any]]:
    conn = _connect(cfg)
    try:
        rows = conn.execute(
            "SELECT id, run_id, job_name, status, detail_json, elapsed_ms, ts FROM job_logs WHERE run_id=? ORDER BY id DESC LIMIT ?",
            (run_id, max(1, min(int(limit or 50), 200))),
        ).fetchall()
        out: List[Dict[str, Any]] = []
        for r in rows:
            out.append(
                {
                    "id": int(r["id"]),
                    "run_id": int(r["run_id"]),
                    "job_name": r["job_name"],
                    "status": r["status"],
                    "detail": _json_load(r["detail_json"], {}),
                    "elapsed_ms": int(r["elapsed_ms"]),
                    "ts": r["ts"],
                }
            )
        return out
    finally:
        conn.close()


def db_health(cfg: DBConfig) -> Dict[str, Any]:
    conn = _connect(cfg)
    try:
        total_runs = int(conn.execute("SELECT COUNT(*) AS c FROM runs").fetchone()["c"])
        total_events = int(conn.execute("SELECT COUNT(*) AS c FROM events").fetchone()["c"])
        total_logs = int(conn.execute("SELECT COUNT(*) AS c FROM job_logs").fetchone()["c"])
        return {
            "db_path": str(cfg.db_path),
            "counts": {"runs": total_runs, "events": total_events, "job_logs": total_logs},
        }
    finally:
        conn.close()


def db_summary(cfg: DBConfig) -> Dict[str, Any]:
    conn = _connect(cfg)
    try:
        total_runs = int(conn.execute("SELECT COUNT(*) AS c FROM runs").fetchone()["c"])
        total_events = int(conn.execute("SELECT COUNT(*) AS c FROM events").fetchone()["c"])
        total_logs = int(conn.execute("SELECT COUNT(*) AS c FROM job_logs").fetchone()["c"])
        stages = conn.execute("SELECT stage, COUNT(*) AS c FROM runs GROUP BY stage ORDER BY c DESC").fetchall()
        return {
            "db_path": str(cfg.db_path),
            "total_runs": total_runs,
            "total_events": total_events,
            "total_job_logs": total_logs,
            "stage_counts": {r["stage"]: int(r["c"]) for r in stages},
        }
    finally:
        conn.close()
