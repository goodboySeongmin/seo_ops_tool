from __future__ import annotations

import json
import os
import re
import time
import html as _html
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from fastapi import FastAPI, Request, HTTPException, Query
from fastapi.responses import HTMLResponse, JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

from fastapi.responses import JSONResponse

try:
    # Pydantic v2
    from pydantic import field_validator  # type: ignore
    _PYDANTIC_V2 = True
except Exception:  # pragma: no cover
    # Pydantic v1
    from pydantic import validator  # type: ignore
    _PYDANTIC_V2 = False

from .db import (
    DBConfig,
    add_event,
    create_run,
    default_db_path,
    get_run,
    init_db,
    list_events,
    update_run,
)
from .services.llm_optimize import optimize_ab_pack
from .services.next_iteration import ab_ctr_summary
from .services.qc_rules import qc_check_text_pack
from .services.seo_audit import build_faq_jsonld, seo_audit_page
from .services.seo_fix import ai_fix_to_pass

# ==========================================================
# App paths
# ==========================================================
APP_ROOT = Path(__file__).resolve().parent
TEMPLATES_DIR = APP_ROOT / "templates"
STATIC_DIR = APP_ROOT / "static"
EXPORT_DIR = APP_ROOT / "exports"
EXPORT_DIR.mkdir(parents=True, exist_ok=True)

# ✅ DB 핸들
DB = DBConfig(db_path=default_db_path(APP_ROOT))
DB_PATH = DB.db_path

# Admin auth (optional)
ADMIN_TOKEN = (os.getenv("ADMIN_TOKEN") or "").strip()

# ==========================================================
# Stage (simple FSM)
# ==========================================================
ST_DRAFT = "DRAFT"
ST_AB_READY = "AB_READY"
ST_APPROVED = "APPROVED"
ST_AUDIT_DONE = "AUDIT_DONE"
ST_FIXED = "FIXED"
ST_EXPORTED = "EXPORTED"

# ==========================================================
# FastAPI
# ==========================================================
app = FastAPI(title="SEO Landing Ops Tool (Google)")
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

def _pick_audit_overall(run: dict) -> str:
    audit = (run or {}).get("audit") or {}
    overall = (audit.get("overall") or "").strip().upper()
    return overall

def _reject_export_not_pass(run_id: int, run: dict) -> JSONResponse:
    overall = _pick_audit_overall(run)
    stage = (run or {}).get("stage")
    return JSONResponse(
        status_code=400,
        content={
            "ok": False,
            "error": "Export requires PASS",
            "hint": "Run Audit → Fix to PASS (or Auto PASS + Export) first.",
            "run_id": run_id,
            "audit_overall": overall or None,
            "stage": stage,
        },
    )

def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _json_error(status_code: int, error: str, **extra):
    payload = {"ok": False, "error": error}
    if extra:
        payload.update(extra)
    return JSONResponse(status_code=status_code, content=payload)


def _admin_guard(request: Request) -> None:
    """ADMIN_TOKEN이 설정된 경우에만 검사. 없으면 오픈."""
    if not ADMIN_TOKEN:
        return
    tok = (request.headers.get("X-Admin-Token") or "").strip()
    if tok != ADMIN_TOKEN:
        raise HTTPException(status_code=401, detail="Unauthorized (bad X-Admin-Token)")


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


# ==========================================================
# job_logs table (admin console용)
# ==========================================================
def _init_job_logs_table() -> None:
    conn = _connect()
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS job_logs (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              run_id INTEGER NOT NULL,
              job_name TEXT NOT NULL,
              status TEXT NOT NULL,
              elapsed_ms INTEGER NOT NULL DEFAULT 0,
              detail_json TEXT NOT NULL DEFAULT '{}',
              ts TEXT NOT NULL,
              FOREIGN KEY(run_id) REFERENCES runs(run_id) ON DELETE CASCADE
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_job_logs_run_id ON job_logs(run_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_job_logs_ts ON job_logs(ts)")
        conn.commit()
    finally:
        conn.close()


def _log_job(run_id: int, job_name: str, status: str, elapsed_ms: int, detail: Dict[str, Any]) -> None:
    conn: Optional[sqlite3.Connection] = None
    try:
        conn = _connect()
        conn.execute(
            "INSERT INTO job_logs(run_id, job_name, status, elapsed_ms, detail_json, ts) VALUES(?,?,?,?,?,?)",
            (run_id, job_name, status, int(elapsed_ms), json.dumps(detail, ensure_ascii=False), _now()),
        )
        conn.commit()
    except Exception:
        pass
    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            pass


def _list_job_logs(run_id: int, limit: int = 50) -> List[Dict[str, Any]]:
    conn = _connect()
    try:
        rows = conn.execute(
            "SELECT id, run_id, job_name, status, elapsed_ms, detail_json, ts "
            "FROM job_logs WHERE run_id=? ORDER BY id DESC LIMIT ?",
            (int(run_id), int(limit)),
        ).fetchall()
        out: List[Dict[str, Any]] = []
        for r in rows:
            try:
                detail = json.loads(r["detail_json"] or "{}")
            except Exception:
                detail = {}
            out.append(
                {
                    "id": int(r["id"]),
                    "run_id": int(r["run_id"]),
                    "job_name": r["job_name"],
                    "status": r["status"],
                    "elapsed_ms": int(r["elapsed_ms"]),
                    "detail": detail,
                    "ts": r["ts"],
                }
            )
        return out
    finally:
        conn.close()


# ==========================================================
# startup
# ==========================================================
@app.on_event("startup")
def _startup():
    init_db(DB)
    _init_job_logs_table()


# ==========================================================
# Models
# ==========================================================
class RunNewPayload(BaseModel):
    meta_title: str = ""
    meta_description: str = ""
    landing_text: str = ""
    primary_keyword: str = ""
    supporting_keywords: List[str] = []
    intent: str = "구매형"
    canonical_url: str = ""
    cta: str = ""
    buy_url: str = ""  # ✅ 확장 필드(일단 run json에 저장)

    if _PYDANTIC_V2:
        @field_validator("supporting_keywords", mode="before")
        @classmethod
        def _parse_supporting_keywords(cls, v):
            if v is None:
                return []
            if isinstance(v, str):
                return [p.strip() for p in re.split(r"[,\n]", v) if p.strip()]
            return v
    else:
        @validator("supporting_keywords", pre=True)
        def _parse_supporting_keywords(cls, v):
            if v is None:
                return []
            if isinstance(v, str):
                return [p.strip() for p in re.split(r"[,\n]", v) if p.strip()]
            return v


class RunIdPayload(BaseModel):
    run_id: int


class EventPayload(BaseModel):
    run_id: int
    variant: str
    event_name: str


class ApprovePayload(BaseModel):
    run_id: int
    variant: str  # "A" | "B" | "RECOMMENDED"


class FixPayload(BaseModel):
    run_id: int
    max_rounds: int = 1


class AutoPassExportPayload(BaseModel):
    run_id: int
    max_rounds: int = 2


def _norm_variant(v: Optional[str]) -> Optional[str]:
    if not v:
        return None
    v = str(v).strip().upper()
    return v if v in ("A", "B") else None


def _pick_opt_variant(opt: Dict[str, Any], variant: str) -> Dict[str, Any]:
    """
    opt["variants"] 지원 형태:
    - dict: {"A": {...}, "B": {...}}
    - list: [{"id":"A", ...}, {"id":"B", ...}] 혹은 {"variant": "A"} 등
    """
    variants = (opt or {}).get("variants")
    if not variants:
        return {}

    if isinstance(variants, dict):
        v = variants.get(variant)
        return v if isinstance(v, dict) else {}

    if isinstance(variants, list):
        key = variant.strip().upper()

        def _get_id(item: Any) -> str:
            if not isinstance(item, dict):
                return ""
            return str(item.get("id") or item.get("variant") or item.get("name") or "").strip().upper()

        for item in variants:
            if _get_id(item) == key and isinstance(item, dict):
                return item

    return {}


# ==========================================================
# Helpers
# ==========================================================
def _make_page_snapshot(
    run: Dict[str, Any], *, variant: Optional[str] = None, base_url: Optional[str] = None
) -> Dict[str, Any]:
    opt = run.get("optimize") or {}
    page = {
        "meta_title": run.get("meta_title", ""),
        "meta_description": run.get("meta_description", ""),
        "canonical_url": run.get("canonical_url", ""),
        "og_title": run.get("og_title", "") or run.get("meta_title", ""),
        "og_description": run.get("og_description", "") or run.get("meta_description", ""),
        "h1": run.get("h1", "") or run.get("primary_keyword", ""),
        "body_html": run.get("body_html", "") or (run.get("landing_text", "") or "").replace("\n", "<br/>"),
        "cta": run.get("cta", ""),
        "faq": run.get("faq", []) or [],
        "has_faq_jsonld": len(run.get("faq", []) or []) >= 3,

        # ✅ 랜딩 확장
        "buy_url": run.get("buy_url", "") or "",
        "products": run.get("products", []) or [],
    }

    vkey = _norm_variant(variant)
    if vkey in ("A", "B"):
        v = _pick_opt_variant(opt, vkey)
        if v:
            page["meta_title"] = v.get("meta_title", page["meta_title"])
            page["meta_description"] = v.get("meta_description", page["meta_description"])
            page["h1"] = v.get("hero_headline", page["h1"]) or page["h1"]
            page["cta"] = v.get("cta", page["cta"])
            page["faq"] = v.get("faq", page["faq"]) or page["faq"]
            page["has_faq_jsonld"] = len(page["faq"]) >= 3

    if not variant:
        approved = run.get("approved")
        if approved and approved.get("variant") in ("A", "B") and opt:
            av = _norm_variant(approved.get("variant"))
            if av:
                v = _pick_opt_variant(opt, av)
                if v:
                    page["meta_title"] = v.get("meta_title", page["meta_title"])
                    page["meta_description"] = v.get("meta_description", page["meta_description"])
                    page["h1"] = v.get("hero_headline", page["h1"]) or page["h1"]
                    page["cta"] = v.get("cta", page["cta"])
                    page["faq"] = v.get("faq", page["faq"]) or page["faq"]
                    page["has_faq_jsonld"] = len(page["faq"]) >= 3

        fixed = run.get("fixed")
        if fixed:
            page.update(
                {
                    "meta_title": fixed.get("meta_title", page["meta_title"]),
                    "meta_description": fixed.get("meta_description", page["meta_description"]),
                    "canonical_url": fixed.get("canonical_url", page["canonical_url"]),
                    "og_title": fixed.get("og_title", page["og_title"]) or fixed.get("meta_title", page["og_title"]),
                    "og_description": fixed.get("og_description", page["og_description"])
                    or fixed.get("meta_description", page["og_description"]),
                    "h1": fixed.get("h1", page["h1"]),
                    "body_html": fixed.get("body_html", page["body_html"]),
                    "cta": fixed.get("cta", page["cta"]),
                    "faq": fixed.get("faq", page["faq"]) or page["faq"],

                    # ✅ FIX 결과에도 buy_url/products 반영
                    "buy_url": fixed.get("buy_url", page.get("buy_url", "")) or page.get("buy_url", ""),
                    "products": fixed.get("products", page.get("products", [])) or page.get("products", []),
                }
            )
            page["has_faq_jsonld"] = len(page.get("faq") or []) >= 3

    if base_url:
        rid = run.get("run_id")
        if rid is not None:
            cur = (page.get("canonical_url") or "").strip()
            if not cur:
                page["canonical_url"] = f"{base_url}/r/{rid}"

    return page


def _render_export_html(
    page: Dict[str, Any],
    *,
    primary_keyword: str,
    supporting_keywords: List[str],
    intent: str,
    canonical_fallback: str = "",
) -> str:
    faq = page.get("faq", []) or []
    if not isinstance(faq, list):
        faq = []

    faq_jsonld = build_faq_jsonld(faq) if len(faq) >= 3 else None

    meta_title = (page.get("meta_title") or "").strip()
    meta_desc = (page.get("meta_description") or "").strip()
    canonical = (page.get("canonical_url") or "").strip() or (canonical_fallback or "")
    og_title = (page.get("og_title") or "").strip() or meta_title
    og_desc = (page.get("og_description") or "").strip() or meta_desc
    h1 = (page.get("h1") or "").strip() or primary_keyword
    body_html = (page.get("body_html") or "").strip()
    cta = (page.get("cta") or "").strip()

    buy_url = (page.get("buy_url") or "").strip()
    cta_href = buy_url if buy_url else "#products"

    products = page.get("products") or []
    if not isinstance(products, list):
        products = []

    chips = []
    if intent:
        chips.append(f"<span class='chip chip-soft'>{_html.escape(intent)}</span>")
    if primary_keyword:
        chips.append(f"<span class='chip chip-solid'>primary: {_html.escape(primary_keyword)}</span>")
    if supporting_keywords:
        chips.append(f"<span class='chip'>keywords: {_html.escape(', '.join(supporting_keywords[:6]))}</span>")
    chips_html = " ".join(chips)

    product_cards = ""
    if products:
        for p in products[:6]:
            if not isinstance(p, dict):
                continue

            name = (p.get("name") or "상품명").strip()
            price = (p.get("price") or "").strip()
            desc = (p.get("desc") or "").strip()
            img = (p.get("img") or "").strip()
            url = (p.get("url") or buy_url or "#buy").strip()

            name_e = _html.escape(name)
            price_e = _html.escape(price)
            desc_e = _html.escape(desc)

            img_tag = (
                f"<div class='pimg' style=\"background-image:url('{img}')\"></div>"
                if img
                else "<div class='pimg ph'></div>"
            )
            price_tag = f"<div class='pprice'>{price_e}</div>" if price else "<div class='pprice muted'>가격 정보</div>"
            desc_tag = f"<div class='pdesc'>{desc_e}</div>" if desc else "<div class='pdesc muted'>한 줄 설명</div>"

            product_cards += f"""
              <a class="pcard" href="{url}" target="_blank" rel="noopener">
                {img_tag}
                <div class="pbody">
                  <div class="ptitle">{name_e}</div>
                  {price_tag}
                  {desc_tag}
                  <div class="pcta">자세히 보기 →</div>
                </div>
              </a>
            """
    else:
        for i in range(3):
            product_cards += f"""
              <div class="pcard" role="article" aria-label="placeholder product">
                <div class="pimg ph"></div>
                <div class="pbody">
                  <div class="ptitle">추천 상품 {i+1} (placeholder)</div>
                  <div class="pprice muted">가격/혜택</div>
                  <div class="pdesc muted">products 데이터(수동 입력/향후 자동 추천)로 실제 카드로 교체됩니다.</div>
                  <div class="pcta">자세히 보기 →</div>
                </div>
              </div>
            """

    faq_html = ""
    if faq:
        faq_items = []
        for it in faq:
            if not isinstance(it, dict):
                continue
            qv = _html.escape(str(it.get("q", "") or ""))
            av = _html.escape(str(it.get("a", "") or ""))
            faq_items.append(f"<div class='faq-item'><div class='q'>Q. {qv}</div><div class='a'>A. {av}</div></div>")

        if faq_items:
            faq_html = f"""
            <section class="card">
              <div class="section-h">FAQ</div>
              <div class="section-sub">자주 묻는 질문을 정리했어요.</div>
              <div class="faq-list">{''.join(faq_items)}</div>
            </section>
            """

    jsonld_block = ""
    if faq_jsonld:
        jsonld_block = f"<script type='application/ld+json'>{json.dumps(faq_jsonld, ensure_ascii=False)}</script>"

    if "구매" in (intent or ""):
        hero_sub = "1분 진단으로 루틴/성분/제품 방향을 빠르게 잡아드려요."
        cta_label = cta or "맞춤 추천 받기"
    else:
        hero_sub = "핵심 정보를 먼저 요약하고, 선택 기준을 정리해드려요."
        cta_label = cta or "핵심 요약 보기"

    meta_title_e = _html.escape(meta_title)
    meta_desc_e = _html.escape(meta_desc)
    canonical_e = _html.escape(canonical)
    og_title_e = _html.escape(og_title)
    og_desc_e = _html.escape(og_desc)
    h1_e = _html.escape(h1)

    return f"""<!doctype html>
<html lang="ko">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>{meta_title_e}</title>
  <meta name="description" content="{meta_desc_e}"/>
  {('<link rel="canonical" href="' + canonical_e + '"/>') if canonical else ""}

  <meta property="og:title" content="{og_title_e}"/>
  <meta property="og:description" content="{og_desc_e}"/>

  {jsonld_block}

  <style>
    :root {{
      --bg:#f6f7fb; --card:#ffffff; --text:#111827; --muted:#6b7280; --line:#e5e7eb;
      --accent:#7c3aed; --accent2:#ec4899;
      --good:#16a34a;
      --radius:18px;
      --shadow:0 10px 28px rgba(17,24,39,.08);
    }}
    *{{box-sizing:border-box}}
    body{{margin:0; font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial; background:var(--bg); color:var(--text);}}
    a{{color:inherit; text-decoration:none}}
    .wrap{{max-width:980px; margin:0 auto; padding:22px 14px 96px;}}
    .hero{{
      background: radial-gradient(1000px 360px at 20% 0%, rgba(236,72,153,.18), transparent 60%),
                  radial-gradient(900px 320px at 80% 0%, rgba(124,58,237,.18), transparent 60%),
                  var(--card);
      border:1px solid var(--line);
      border-radius:24px;
      box-shadow:var(--shadow);
      padding:18px 18px 16px;
    }}
    .chips{{display:flex; gap:8px; flex-wrap:wrap; margin-bottom:10px}}
    .chip{{font-size:12px; padding:6px 10px; border-radius:999px; border:1px solid var(--line); background:#fff; color:var(--text)}}
    .chip-soft{{background:rgba(124,58,237,.08); border-color:rgba(124,58,237,.22)}}
    .chip-solid{{background:rgba(236,72,153,.10); border-color:rgba(236,72,153,.20)}}
    .h1{{font-size:34px; font-weight:900; margin:6px 0 6px}}
    .sub{{color:var(--muted); margin:0 0 14px; line-height:1.5}}
    .hero-actions{{display:flex; gap:10px; flex-wrap:wrap}}
    .btn{{border:1px solid var(--line); background:#fff; padding:10px 14px; border-radius:14px; font-weight:800; font-size:13px; cursor:pointer}}
    .btn.primary{{background:linear-gradient(135deg,var(--accent2),var(--accent)); border:none; color:#fff}}
    .btn.ghost{{background:#fff}}
    .grid2{{display:grid; grid-template-columns: 1fr 1fr; gap:12px; margin-top:12px}}
    @media (max-width:900px){{ .grid2{{grid-template-columns:1fr}} .h1{{font-size:28px}} }}

    .card{{
      margin-top:12px;
      background:var(--card);
      border:1px solid var(--line);
      border-radius:var(--radius);
      box-shadow:var(--shadow);
      padding:14px;
    }}
    .section-h{{font-size:15px; font-weight:900; margin-bottom:6px}}
    .section-sub{{font-size:12px; color:var(--muted); margin-bottom:10px}}
    .note{{font-size:12px; color:var(--muted); line-height:1.5}}

    .pillrow{{display:flex; gap:8px; flex-wrap:wrap}}
    .pill{{border:1px solid var(--line); border-radius:999px; padding:8px 10px; font-size:12px; font-weight:800; background:#fff}}
    .pill.on{{border-color:rgba(236,72,153,.35); box-shadow:0 0 0 3px rgba(236,72,153,.14)}}

    .products{{display:grid; grid-template-columns:repeat(3,1fr); gap:12px}}
    @media (max-width:900px){{ .products{{grid-template-columns:1fr}} }}
    .pcard{{
      display:block;
      background:#fff;
      border:1px solid var(--line);
      border-radius:18px;
      overflow:hidden;
      box-shadow:0 8px 22px rgba(17,24,39,.06);
      transition:transform .08s ease;
    }}
    .pcard:hover{{transform:translateY(-2px)}}
    .pimg{{height:140px; background-size:cover; background-position:center;}}
    .pimg.ph{{background:linear-gradient(135deg, rgba(236,72,153,.18), rgba(124,58,237,.18));}}
    .pbody{{padding:12px}}
    .ptitle{{font-weight:900; margin-bottom:6px}}
    .pprice{{font-weight:900; color:#111827; margin-bottom:6px}}
    .pdesc{{font-size:12px; color:#374151; line-height:1.45}}
    .muted{{color:var(--muted)}}
    .pcta{{margin-top:10px; font-size:12px; font-weight:900; color:var(--accent)}}

    .faq-list{{display:grid; gap:10px}}
    .faq-item{{border:1px solid #f1f5f9; border-radius:14px; padding:12px}}
    .faq-item .q{{font-weight:900; margin-bottom:6px}}
    .faq-item .a{{color:#374151; line-height:1.55}}

    .sticky{{
      position:fixed; left:0; right:0; bottom:0;
      background:rgba(246,247,251,.88);
      backdrop-filter: blur(10px);
      border-top:1px solid var(--line);
      padding:12px 14px;
    }}
    .sticky .bar{{max-width:980px; margin:0 auto; display:flex; gap:10px; align-items:center}}
    .sticky .meta{{flex:1; font-size:12px; color:var(--muted)}}
    .sticky a{{display:inline-flex; align-items:center; justify-content:center}}
  </style>
</head>
<body>
  <div class="wrap">
    <section class="hero">
      <div class="chips">{chips_html}</div>
      <div class="h1">{h1_e}</div>
      <p class="sub">{_html.escape(hero_sub)}</p>
      <div class="hero-actions">
        <a class="btn primary" href="{cta_href}">{_html.escape(cta_label)}</a>
        <a class="btn ghost" href="#summary">추천 요약</a>
        <a class="btn ghost" href="#products">추천 상품</a>
      </div>

      <div class="grid2">
        <div class="card" style="margin-top:12px">
          <div class="section-h">오늘의 추천 포인트</div>
          <div class="note">
            “{_html.escape(primary_keyword)}”는 자극/건조/피지 밸런스에 따라 체감이 달라요. 먼저 <b>장벽/진정/수분</b> 밸런스를 잡고, 그 다음 타깃 케어를 추천해요.
          </div>
        </div>
        <div class="card" style="margin-top:12px">
          <div class="section-h">미미박스 톤 & 운영형 랜딩</div>
          <div class="note">
            짧은 진단 → 추천 루틴/성분 → 추천 상품 → CTA로 이어지는 전환 구조를 기본으로 합니다.
            (지금은 운영툴 템플릿이며, 상품 데이터가 들어오면 완성도가 급상승합니다)
          </div>
        </div>
      </div>
    </section>

    <section class="card" id="diagnosis">
      <div class="section-h">1분 피부고민 셀렉터 (샘플 UI)</div>
      <div class="section-sub">DB 없이도 “랜딩다운 흐름”을 보여주기 위한 최소 구성입니다.</div>
      <div class="pillrow">
        <span class="pill on">보습</span>
        <span class="pill">민감</span>
        <span class="pill">피부장벽</span>
      </div>
      <div class="note" style="margin-top:10px">
        보습 중심으로 먼저 정리해볼게요. 우선순위는 장벽/진정/수분 → 다음으로 타깃 케어(보습) 순서가 안정적이에요.
      </div>
    </section>

    <section class="card" id="summary">
      <div class="section-h">추천 요약</div>
      <div class="section-sub">고민의 “원인/자극/루틴”을 먼저 정리하고 제품으로 연결합니다.</div>
      <div class="note">{body_html or "여기에 추천 요약 콘텐츠가 들어갑니다. (현재는 placeholder / FIX 결과가 있으면 더 좋아집니다.)"}</div>
    </section>

    <section class="card" id="products">
      <div class="section-h">추천 상품</div>
      <div class="section-sub">products 데이터가 있으면 실제 카드로 렌더되고, 없으면 placeholder 카드가 표시됩니다.</div>
      <div class="products">
        {product_cards}
      </div>
    </section>

    {faq_html}
  </div>

  <div class="sticky">
    <div class="bar">
      <div class="meta">CTA를 “실제 buy_url”로 연결하면 운영툴 완성도가 확 올라갑니다.</div>
      <a class="btn primary" href="{cta_href}">{_html.escape(cta_label)}</a>
    </div>
  </div>
</body>
</html>
"""


def _diff_before_after(before_page: Dict[str, Any], after_page: Dict[str, Any]) -> Dict[str, Any]:
    keys = ["meta_title", "meta_description", "canonical_url", "og_title", "og_description", "h1", "cta"]
    changed: Dict[str, Any] = {}
    for k in keys:
        b = (before_page.get(k) or "").strip()
        a = (after_page.get(k) or "").strip()
        if b != a:
            changed[k] = {"before": b, "after": a}

    before_faq = before_page.get("faq") or []
    after_faq = after_page.get("faq") or []
    return {"changed": changed, "faq": {"before_count": len(before_faq), "after_count": len(after_faq)}}


# ==========================================================
# Enforce layer: make WARN -> PASS
# ==========================================================
_TAG_RE = re.compile(r"<[^>]+>")


def _strip_html(s: str) -> str:
    s = (s or "").replace("<br/>", " ").replace("<br>", " ")
    s = _TAG_RE.sub(" ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _fit_len(text: str, min_len: int, max_len: int, *, pad: str = "", cut_ellipsis: bool = False) -> str:
    t = (text or "").strip()
    if len(t) < min_len:
        if pad:
            t = (t + " " + pad).strip() if t else pad.strip()
    if len(t) > max_len:
        t = t[:max_len].rstrip()
        if cut_ellipsis and len(t) >= 1 and t[-1] not in ".!?":
            t = (t[:-1] + "…").strip()
    return t


def _ensure_sentence_end(text: str, max_len: int) -> str:
    t = (text or "").strip()
    if not t:
        return t
    if t[-1] in ".!?":
        return t
    if len(t) + 1 <= max_len:
        return (t + ".").strip()
    if max_len >= 1:
        t = t[:max_len].rstrip()
        if t and t[-1] not in ".!?":
            t = (t[:-1] + ".").strip()
    return t


def _ensure_kw_in_h1(h1: str, primary_kw: str) -> str:
    h = (h1 or "").strip()
    kw = (primary_kw or "").strip()
    if not kw:
        return h
    if not h:
        return kw
    if kw in h:
        return h
    return f"{kw} {h}".strip()


def _ensure_kw_in_first120(body_html: str, primary_kw: str, supporting: List[str], intent: str) -> str:
    kw = (primary_kw or "").strip()
    if not kw:
        return body_html

    plain = _strip_html(body_html or "")
    if len(plain) >= 1 and kw in plain[:120]:
        return body_html

    sup = [s.strip() for s in (supporting or []) if s and s.strip()]
    sup_part = ""
    if sup:
        sup_part = " (" + ", ".join(sup[:3]) + ")"

    intent = (intent or "").strip()
    if "구매" in intent:
        lead = f"<p><b>{kw}</b>{sup_part} 기준으로 선택 포인트와 사용 팁을 정리했습니다. 아래에서 확인해 보세요.</p>"
    else:
        lead = f"<p><b>{kw}</b>{sup_part} 핵심 정보를 먼저 요약합니다. 아래에서 자세히 확인해 보세요.</p>"

    return (lead + (body_html or "")).strip()


def _enforce_page_for_pass(
    page: Dict[str, Any],
    primary_keyword: str,
    supporting_keywords: List[str],
    intent: str,
) -> Dict[str, Any]:
    p = dict(page or {})
    kw = (primary_keyword or "").strip()
    sup = [s.strip() for s in (supporting_keywords or []) if s and s.strip()]

    title = (p.get("meta_title") or "").strip()
    if kw and kw not in title:
        title = f"{kw} {title}".strip() if title else f"{kw} 선택 가이드".strip()

    title_pad = ""
    if kw:
        title_pad = f"| {kw} 구매 가이드" if ("구매" in (intent or "")) else f"| {kw} 핵심 정리"
    title = _fit_len(title, 30, 60, pad=title_pad)

    desc = (p.get("meta_description") or "").strip()
    if kw and kw not in desc:
        desc = (f"{kw} " + desc).strip() if desc else f"{kw} 정보를 정리했습니다."

    desc_pad = "선택 기준, 사용 팁, FAQ까지 과장 없이 정리했습니다. 지금 확인해 보세요"
    desc = _fit_len(desc, 70, 160, pad=desc_pad)
    desc = _ensure_sentence_end(desc, 160)

    h1 = (p.get("h1") or "").strip()
    h1 = _ensure_kw_in_h1(h1, kw)
    if not h1 and kw:
        h1 = kw

    body_html = (p.get("body_html") or "").strip()
    body_html = _ensure_kw_in_first120(body_html, kw, sup, intent)

    og_title = (p.get("og_title") or "").strip() or title
    og_desc = (p.get("og_description") or "").strip() or desc

    p["meta_title"] = title
    p["meta_description"] = desc
    p["h1"] = h1
    p["body_html"] = body_html
    p["og_title"] = og_title
    p["og_description"] = og_desc

    faq = p.get("faq", []) or []
    p["has_faq_jsonld"] = len(faq) >= 3
    return p


def _run_summary(run: Dict[str, Any]) -> Dict[str, Any]:
    audit = run.get("audit") or {}
    return {
        "run_id": run.get("run_id"),
        "primary_keyword": run.get("primary_keyword", ""),
        "intent": run.get("intent", ""),
        "stage": run.get("stage", ""),
        "updated_at": run.get("updated_at") or run.get("updatedAt") or run.get("updated") or "",
        "approved": run.get("approved") or None,
        "audit": {
            "overall": audit.get("overall"),
            "score": audit.get("score"),
            "signals": (audit.get("signals") or {}),
            "issues": audit.get("issues") or [],
        }
        if audit
        else None,
        "export": run.get("export") or None,
    }


def _list_run_ids_basic(q: str, stage: str, limit: int, offset: int) -> List[int]:
    conn = _connect()
    try:
        sql = "SELECT run_id FROM runs"
        where = []
        params: List[Any] = []
        if q.strip():
            where.append("(meta_title LIKE ? OR primary_keyword LIKE ?)")
            like = f"%{q.strip()}%"
            params += [like, like]
        if stage.strip():
            where.append("stage = ?")
            params.append(stage.strip())
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY updated_at DESC LIMIT ? OFFSET ?"
        params += [int(limit), int(offset)]
        rows = conn.execute(sql, params).fetchall()
        return [int(r["run_id"]) for r in rows]
    finally:
        conn.close()


# ==========================================================
# UI pages
# ==========================================================
@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/admin", response_class=HTMLResponse)
def admin_home(request: Request):
    return templates.TemplateResponse("admin.html", {"request": request})


# ==========================================================
# Public preview by run_id (+ variant=A|B)
# ==========================================================
@app.get("/r/{run_id}", response_class=HTMLResponse)
def public_preview(
    run_id: int,
    request: Request,
    variant: str = Query(default="", description="A/B preview: ?variant=A or ?variant=B"),
):
    try:
        run = get_run(DB, run_id)
    except KeyError:
        return HTMLResponse("<h1>404 Not Found</h1>", status_code=404)

    primary = run.get("primary_keyword", "") or "랜딩 페이지"
    supporting = run.get("supporting_keywords", []) or []
    intent = run.get("intent", "구매형")

    base_url = str(request.base_url).rstrip("/")
    v = (variant or "").strip().upper()
    page_variant = v if v in ("A", "B") else None

    page = _make_page_snapshot(run, variant=page_variant, base_url=base_url)
    html = _render_export_html(
        page,
        primary_keyword=primary,
        supporting_keywords=supporting,
        intent=intent,
        canonical_fallback=f"{base_url}/r/{run_id}",
    )
    return HTMLResponse(html)


# ==========================================================
# Public API: Run CRUD
# ==========================================================
@app.post("/api/run/new")
def api_run_new(payload: RunNewPayload):
    t0 = time.perf_counter()
    try:
        run = create_run(
            DB,
            meta_title=payload.meta_title or "",
            meta_description=payload.meta_description or "",
            landing_text=payload.landing_text or "",
            primary_keyword=payload.primary_keyword or "",
            supporting_keywords=payload.supporting_keywords or [],
            intent=payload.intent or "구매형",
            canonical_url=payload.canonical_url or "",
            cta=payload.cta or "",
        )

        # ✅ buy_url은 스키마 변경 없이 run json 필드로 저장
        if (payload.buy_url or "").strip():
            run = update_run(DB, int(run["run_id"]), fields={"buy_url": payload.buy_url.strip()})

        elapsed = int((time.perf_counter() - t0) * 1000)
        _log_job(int(run["run_id"]), "RUN_NEW", "OK", elapsed, {"run_id": run["run_id"]})
        return {"ok": True, "run": run}
    except Exception as e:
        elapsed = int((time.perf_counter() - t0) * 1000)
        _log_job(0, "RUN_NEW", "ERR", elapsed, {"error": str(e)})
        return _json_error(500, f"run/new failed: {e}")


@app.get("/api/run/{run_id}")
def api_get_run(run_id: int):
    try:
        run = get_run(DB, run_id)
    except KeyError as e:
        return _json_error(404, str(e))
    run["events"] = list_events(DB, run_id)
    return {"ok": True, "run": run}


@app.get("/api/runs")
def api_list_runs(
    q: str = Query(default="", description="search in title/kw"),
    stage: str = Query(default="", description="filter by stage"),
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
):
    conn = _connect()
    try:
        sql = "SELECT run_id FROM runs"
        where = []
        params: List[Any] = []
        if q.strip():
            where.append("(meta_title LIKE ? OR primary_keyword LIKE ?)")
            like = f"%{q.strip()}%"
            params += [like, like]
        if stage.strip():
            where.append("stage = ?")
            params.append(stage.strip())
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY updated_at DESC LIMIT ? OFFSET ?"
        params += [int(limit), int(offset)]
        rows = conn.execute(sql, params).fetchall()
        ids = [int(r["run_id"]) for r in rows]
    finally:
        conn.close()

    runs = []
    for rid in ids:
        try:
            runs.append(get_run(DB, rid))
        except KeyError:
            continue
    return {"ok": True, "items": runs, "count": len(runs), "limit": limit, "offset": offset}


# ==========================================================
# Public API: A/B Optimize
# ==========================================================
@app.post("/api/optimize_ab")
def api_optimize_ab(payload: RunIdPayload):
    t0 = time.perf_counter()
    try:
        run = get_run(DB, payload.run_id)
    except KeyError as e:
        return _json_error(404, str(e))

    primary = run.get("primary_keyword", "")
    supporting = run.get("supporting_keywords", []) or []
    intent = run.get("intent", "구매형")

    pack = optimize_ab_pack(
        primary_keyword=primary,
        supporting_keywords=supporting,
        intent=intent,
        meta_title=run.get("meta_title", ""),
        meta_description=run.get("meta_description", ""),
        landing_text=run.get("landing_text", ""),
    )

    variants = pack.get("variants", {}) or {}
    qc: Dict[str, Any] = {}
    for k, v in variants.items():
        joined = (
            f"{v.get('meta_title','')}\n{v.get('meta_description','')}\n{v.get('hero_headline','')}\n"
            f"{v.get('hero_sub','')}\n{v.get('cta','')}"
        )
        qc[k] = qc_check_text_pack(joined)

    run = update_run(DB, payload.run_id, fields={"optimize": pack, "qc": qc, "stage": ST_AB_READY})
    run["events"] = list_events(DB, payload.run_id)

    elapsed = int((time.perf_counter() - t0) * 1000)
    _log_job(payload.run_id, "OPTIMIZE_AB", "OK", elapsed, {"qc": qc})
    return {"ok": True, "run_id": payload.run_id, "optimize": pack, "qc": qc, "run": run}


# ==========================================================
# Public API: Events / CTR
# ==========================================================
@app.post("/api/event")
def api_event(payload: EventPayload):
    try:
        _ = get_run(DB, payload.run_id)
    except KeyError as e:
        return _json_error(404, str(e))

    if payload.variant not in ("A", "B"):
        return _json_error(400, "variant must be A or B")
    if payload.event_name not in ("view", "cta_click"):
        return _json_error(400, "event_name must be view or cta_click")

    ev = add_event(DB, run_id=payload.run_id, variant=payload.variant, event_name=payload.event_name)
    return {"ok": True, "event": ev}


@app.get("/api/ctr_summary")
def api_ctr_summary(run_id: int):
    try:
        _ = get_run(DB, run_id)
    except KeyError as e:
        return _json_error(404, str(e))
    events = list_events(DB, run_id)
    summary = ab_ctr_summary(events)
    return {"ok": True, "run_id": run_id, "summary": summary}


# ==========================================================
# Public API: Approve
# ==========================================================
@app.post("/api/approve")
def api_approve(payload: ApprovePayload):
    t0 = time.perf_counter()
    try:
        _ = get_run(DB, payload.run_id)
    except KeyError as e:
        return _json_error(404, str(e))

    if payload.variant == "RECOMMENDED":
        events = list_events(DB, payload.run_id)
        summary = ab_ctr_summary(events)
        v = summary.get("recommend_variant")

        if v not in ("A", "B"):
            min_views = int(summary.get("min_views_required", 0) or 0)
            a_view = int(summary.get("A", {}).get("view", 0) or 0)
            b_view = int(summary.get("B", {}).get("view", 0) or 0)
            if min_views > 0 and a_view >= min_views and b_view >= min_views:
                a_ctr = float(summary.get("A", {}).get("ctr", 0.0) or 0.0)
                b_ctr = float(summary.get("B", {}).get("ctr", 0.0) or 0.0)
                v = "A" if a_ctr >= b_ctr else "B"
                approved = {"variant": v, "ts": _now(), "method": "recommended_fallback_max_ctr", "summary": summary}
            else:
                return _json_error(
                    400,
                    "No recommended variant yet (need more events or significance).",
                    summary=summary,
                    hint=f"Record View/CTA Click을 더 쌓아주세요. (min_views_required={min_views})",
                )
        else:
            approved = {"variant": v, "ts": _now(), "method": "recommended", "summary": summary}
    else:
        if payload.variant not in ("A", "B"):
            return _json_error(400, "variant must be A or B or RECOMMENDED")
        approved = {"variant": payload.variant, "ts": _now(), "method": "manual"}

    run = update_run(DB, payload.run_id, fields={"approved": approved, "stage": ST_APPROVED})
    elapsed = int((time.perf_counter() - t0) * 1000)
    _log_job(payload.run_id, "APPROVE", "OK", elapsed, {"approved": approved})
    return {"ok": True, "run_id": payload.run_id, "approved": run.get("approved")}


# ==========================================================
# Public API: SEO Audit / Fix / Export
# ==========================================================
@app.post("/api/seo_audit")
def api_seo_audit(payload: RunIdPayload, request: Request):
    t0 = time.perf_counter()
    try:
        run = get_run(DB, payload.run_id)
    except KeyError as e:
        return _json_error(404, str(e))

    base_url = str(request.base_url).rstrip("/")
    page = _make_page_snapshot(run, variant=None, base_url=base_url)
    primary = run.get("primary_keyword", "")
    supporting = run.get("supporting_keywords", []) or []
    intent = run.get("intent", "구매형")

    audit = seo_audit_page(page, primary, supporting, intent)
    run = update_run(DB, payload.run_id, fields={"audit": audit, "stage": ST_AUDIT_DONE})
    elapsed = int((time.perf_counter() - t0) * 1000)
    _log_job(payload.run_id, "SEO_AUDIT", "OK", elapsed, {"audit": audit})
    return {"ok": True, "run_id": payload.run_id, "audit": audit, "run": run}


@app.post("/api/fix_to_pass")
def api_fix_to_pass(payload: FixPayload, request: Request):
    t0 = time.perf_counter()
    try:
        run = get_run(DB, payload.run_id)
    except KeyError as e:
        return _json_error(404, str(e))

    primary = run.get("primary_keyword", "")
    supporting = run.get("supporting_keywords", []) or []
    intent = run.get("intent", "구매형")

    base_url = str(request.base_url).rstrip("/")

    before_page = _make_page_snapshot(run, variant=None, base_url=base_url)
    before_audit = seo_audit_page(before_page, primary, supporting, intent)

    if before_audit["overall"] == "PASS":
        run = update_run(DB, payload.run_id, fields={"audit": before_audit, "stage": ST_AUDIT_DONE})
        elapsed = int((time.perf_counter() - t0) * 1000)
        _log_job(payload.run_id, "FIX_TO_PASS", "OK", elapsed, {"status": "already_pass", "audit": before_audit})
        return {"ok": True, "run_id": payload.run_id, "status": "already_pass", "audit": before_audit, "run": run}

    fixed_page = None
    last_audit = before_audit
    rounds = max(1, min(int(payload.max_rounds or 1), 6))

    page = before_page
    for _ in range(rounds):
        fixed_page = ai_fix_to_pass(
            page=page,
            audit=last_audit,
            primary_keyword=primary,
            supporting_keywords=supporting,
            intent=intent,
            run_id=payload.run_id,
            base_url=base_url,
        )
        fixed_page = _enforce_page_for_pass(fixed_page, primary, supporting, intent)
        last_audit = seo_audit_page(fixed_page, primary, supporting, intent)
        page = fixed_page
        if last_audit["overall"] == "PASS":
            break

    run = update_run(DB, payload.run_id, fields={"fixed": fixed_page, "audit": last_audit, "stage": ST_FIXED})
    diff = _diff_before_after(before_page, fixed_page or {}) if fixed_page else None

    elapsed = int((time.perf_counter() - t0) * 1000)
    _log_job(
        payload.run_id,
        "FIX_TO_PASS",
        "OK",
        elapsed,
        {"before_audit": before_audit, "after_audit": last_audit, "diff": diff},
    )

    return {
        "ok": True,
        "run_id": payload.run_id,
        "before": {"page": before_page, "audit": before_audit},
        "fixed": fixed_page,
        "audit": last_audit,
        "diff": diff,
    }


@app.post("/api/export")
def api_export(payload: RunIdPayload, request: Request):
    t0 = time.perf_counter()
    try:
        run = get_run(DB, payload.run_id)
    except KeyError as e:
        return _json_error(404, str(e))

    audit = run.get("audit") or {}
    overall = (audit.get("overall") or "UNKNOWN").strip().upper()

    # ✅ 서버 가드: PASS 아니면 무조건 차단
    if overall != "PASS":
        _log_job(payload.run_id, "EXPORT", "ERR", 0, {"error": f"blocked (current={overall})", "audit": audit})
        return _json_error(
            400,
            f"Export blocked: requires PASS (current={overall}). Run Audit → Fix/Auto first.",
            audit=audit,
        )

    primary = run.get("primary_keyword", "")
    supporting = run.get("supporting_keywords", []) or []
    intent = run.get("intent", "구매형")

    base_url = str(request.base_url).rstrip("/")
    page = _make_page_snapshot(run, variant=None, base_url=base_url)
    html = _render_export_html(
        page,
        primary_keyword=primary,
        supporting_keywords=supporting,
        intent=intent,
        canonical_fallback=f"{base_url}/r/{payload.run_id}",
    )

    out = EXPORT_DIR / f"run_{payload.run_id}.html"
    out.write_text(html, encoding="utf-8")

    export = {"path": str(out), "ts": _now()}
    run = update_run(DB, payload.run_id, fields={"export": export, "stage": ST_EXPORTED})

    elapsed = int((time.perf_counter() - t0) * 1000)
    _log_job(payload.run_id, "EXPORT", "OK", elapsed, {"export": export})
    return {"ok": True, "run_id": payload.run_id, "export_path": str(out), "export": export, "run": run}


@app.post("/api/auto_pass_export")
def api_auto_pass_export(payload: AutoPassExportPayload, request: Request):
    t0 = time.perf_counter()
    try:
        run = get_run(DB, payload.run_id)
    except KeyError as e:
        return _json_error(404, str(e))

    base_url = str(request.base_url).rstrip("/")
    primary = run.get("primary_keyword", "")
    supporting = run.get("supporting_keywords", []) or []
    intent = run.get("intent", "구매형")

    before_page = _make_page_snapshot(run, variant=None, base_url=base_url)
    before_audit = seo_audit_page(before_page, primary, supporting, intent)

    fixed_page = None
    last_audit = before_audit

    rounds = max(1, min(int(payload.max_rounds or 2), 6))
    page = before_page

    if before_audit.get("overall") != "PASS":
        for _ in range(rounds):
            fixed_page = ai_fix_to_pass(
                page=page,
                audit=last_audit,
                primary_keyword=primary,
                supporting_keywords=supporting,
                intent=intent,
                run_id=payload.run_id,
                base_url=base_url,
            )
            fixed_page = _enforce_page_for_pass(fixed_page, primary, supporting, intent)
            last_audit = seo_audit_page(fixed_page, primary, supporting, intent)
            page = fixed_page
            if last_audit.get("overall") == "PASS":
                break

        run = update_run(DB, payload.run_id, fields={"fixed": fixed_page, "audit": last_audit, "stage": ST_FIXED})
    else:
        run = update_run(DB, payload.run_id, fields={"audit": before_audit, "stage": ST_AUDIT_DONE})

    diff = _diff_before_after(before_page, fixed_page or {}) if fixed_page else None

    if (run.get("audit") or {}).get("overall") != "PASS":
        elapsed = int((time.perf_counter() - t0) * 1000)
        _log_job(
            payload.run_id,
            "AUTO_PASS_EXPORT",
            "ERR",
            elapsed,
            {"before_audit": before_audit, "after_audit": run.get("audit"), "diff": diff},
        )
        return _json_error(
            400,
            "Auto PASS + Export blocked: still not PASS",
            audit=run.get("audit"),
            diff=diff,
            fixed=fixed_page,
        )

    page2 = _make_page_snapshot(run, variant=None, base_url=base_url)
    html = _render_export_html(
        page2,
        primary_keyword=primary,
        supporting_keywords=supporting,
        intent=intent,
        canonical_fallback=f"{base_url}/r/{payload.run_id}",
    )

    out = EXPORT_DIR / f"run_{payload.run_id}.html"
    out.write_text(html, encoding="utf-8")

    export = {"path": str(out), "ts": _now()}
    run = update_run(DB, payload.run_id, fields={"export": export, "stage": ST_EXPORTED})

    export_url = f"/api/export/open?run_id={payload.run_id}"

    elapsed = int((time.perf_counter() - t0) * 1000)
    _log_job(
        payload.run_id,
        "AUTO_PASS_EXPORT",
        "OK",
        elapsed,
        {"before_audit": before_audit, "after_audit": run.get("audit"), "diff": diff, "export": export},
    )

    return {
        "ok": True,
        "run_id": payload.run_id,
        "audit": run.get("audit"),
        "fixed": fixed_page,
        "diff": diff,
        "export": export,
        "export_url": export_url,
        "run": run,
    }


@app.get("/api/export/open", response_class=HTMLResponse)
def api_export_open(run_id: int):
    try:
        run = get_run(DB, run_id)
    except KeyError:
        return HTMLResponse("<h1>404</h1>", status_code=404)

    ex = run.get("export") or {}
    path = ex.get("path")
    if not path or not Path(path).exists():
        return HTMLResponse("<h1>No export yet</h1>", status_code=404)

    html = Path(path).read_text(encoding="utf-8")
    return HTMLResponse(html)


@app.get("/api/export/file")
def api_export_file(run_id: int):
    try:
        run = get_run(DB, run_id)
    except KeyError:
        return _json_error(404, "run not found")

    ex = run.get("export") or {}
    path = ex.get("path")
    if not path or not Path(path).exists():
        return _json_error(404, "no export yet")

    return FileResponse(path, media_type="text/html", filename=Path(path).name)


# ==========================================================
# Admin APIs (guarded)
# ==========================================================
@app.get("/api/admin/health")
def api_admin_health(request: Request):
    _admin_guard(request)
    try:
        conn = _connect()
        conn.execute("SELECT 1").fetchone()
        conn.close()
        return {"ok": True, "db_path": str(DB_PATH)}
    except Exception as e:
        return _json_error(500, f"health failed: {e}")


@app.get("/api/admin/logs")
def api_admin_logs(request: Request, run_id: int, limit: int = 50):
    _admin_guard(request)
    items = _list_job_logs(run_id, limit=limit)
    return {"ok": True, "run_id": run_id, "items": items}


@app.get("/api/admin/runs_v2")
def api_admin_runs_v2(
    request: Request,
    limit: int = 50,
    offset: int = 0,
    q: str = "",
    stage: str = "",
    audit_overall: str = "",  # PASS | WARN | FAIL
    sort: str = "updated_desc",  # updated_desc | score_desc | run_id_desc
):
    _admin_guard(request)

    ids = _list_run_ids_basic(q=q or "", stage=stage or "", limit=int(limit), offset=int(offset))
    items: List[Dict[str, Any]] = []
    for rid in ids:
        try:
            run = get_run(DB, rid)
        except KeyError:
            continue
        items.append(_run_summary(run))

    ao = (audit_overall or "").strip().upper()
    if ao in ("PASS", "WARN", "FAIL"):
        items = [it for it in items if (it.get("audit") or {}).get("overall") == ao]

    s = (sort or "").strip()
    if s == "score_desc":
        items.sort(key=lambda x: ((x.get("audit") or {}).get("score") or -1), reverse=True)
    elif s == "run_id_desc":
        items.sort(key=lambda x: (x.get("run_id") or 0), reverse=True)

    return {"ok": True, "items": items, "count": len(items), "limit": limit, "offset": offset}


@app.get("/api/admin/run/{run_id}")
def api_admin_run_detail(request: Request, run_id: int):
    _admin_guard(request)
    try:
        run = get_run(DB, run_id)
    except KeyError:
        return _json_error(404, "run not found")
    return {"ok": True, "run": run}


# ==========================================================
# Admin CTR Summary (stable default)
# - 기본: list_events(DB) + ab_ctr_summary(events)
# - 옵션: use_raw=1 이면 sqlite에서 테이블 auto-detect로 집계 (실험용)
# ==========================================================
def _qident(name: str) -> str:
    # sqlite identifier safe quoting
    return '"' + name.replace('"', '""') + '"'


def _detect_events_table(conn: sqlite3.Connection) -> Tuple[str, str, str, str]:
    cur = conn.cursor()
    tables = [r[0] for r in cur.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]

    preferred = ["events", "run_events", "event_logs", "ab_events", "user_events"]
    ordered = [t for t in preferred if t in tables] + [t for t in tables if t not in preferred]

    def cols_of(t: str):
        cols = [r[1] for r in cur.execute(f"PRAGMA table_info({_qident(t)})").fetchall()]
        lower = [c.lower() for c in cols]
        return cols, lower

    run_candidates = ["run_id", "runid", "rid"]
    variant_candidates = ["variant", "ab_variant", "bucket"]
    event_candidates = ["event", "event_name", "name", "type", "action"]

    for t in ordered:
        cols, lower = cols_of(t)

        def pick(cands):
            for c in cands:
                if c in lower:
                    return cols[lower.index(c)]
            return None

        run_col = pick(run_candidates)
        var_col = pick(variant_candidates)
        evt_col = pick(event_candidates)

        if run_col and var_col and evt_col:
            return (t, run_col, var_col, evt_col)

    raise RuntimeError("events table not found (need columns like run_id + variant + event_name)")


def _ctr_summary_from_db(db_path: str, run_id: int) -> Dict[str, Any]:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        table, run_col, var_col, evt_col = _detect_events_table(conn)
        cur = conn.cursor()

        tq = _qident(table)
        runq = _qident(run_col)
        varq = _qident(var_col)
        evtq = _qident(evt_col)

        rows = cur.execute(
            f"""
            SELECT
              {varq} AS variant,
              {evtq} AS ev
            FROM {tq}
            WHERE {runq} = ?
            """,
            (int(run_id),),
        ).fetchall()

        stats: Dict[str, Dict[str, int]] = {}
        for r in rows:
            v = str(r["variant"] or "").strip().upper()
            if not v:
                continue
            ev = str(r["ev"] or "").strip().lower()
            if v not in stats:
                stats[v] = {"views": 0, "clicks": 0}
            if "click" in ev:
                stats[v]["clicks"] += 1
            elif "view" in ev:
                stats[v]["views"] += 1

        def calc_ctr(vv: int, cc: int) -> float:
            return (cc / vv) if vv > 0 else 0.0

        out_variants: Dict[str, Dict[str, Any]] = {}
        for v, s in stats.items():
            vv, cc = int(s["views"]), int(s["clicks"])
            out_variants[v] = {"views": vv, "clicks": cc, "ctr": calc_ctr(vv, cc)}

        rec = None
        reason = "insufficient_data"
        if out_variants:
            candidates = [k for k in ["A", "B"] if k in out_variants] or list(out_variants.keys())
            min_views = 5
            scored = [(k, float(out_variants[k]["ctr"]), int(out_variants[k]["views"])) for k in candidates]
            scored2 = [x for x in scored if x[2] >= min_views]
            pick_from = scored2 if scored2 else scored
            pick_from.sort(key=lambda x: (x[1], x[2]), reverse=True)
            rec = pick_from[0][0]
            reason = "highest_ctr"

        return {
            "run_id": run_id,
            "variants": out_variants,
            "recommended": rec,
            "reason": reason,
            "ts": _now(),
            "source_table": table,
        }
    finally:
        conn.close()


@app.get("/api/admin/ctr_summary")
def admin_ctr_summary(run_id: int, request: Request, use_raw: int = 0):
    _admin_guard(request)
    try:
        # 기본은 기존 로직(프로젝트 일관성)
        if not use_raw:
            events = list_events(DB, int(run_id))
            summary = ab_ctr_summary(events)
            summary["ts"] = _now()
            summary["source"] = "list_events+ab_ctr_summary"
            return {"ok": True, "summary": summary}

        # 실험용: sqlite raw detect
        summary2 = _ctr_summary_from_db(str(DB_PATH), int(run_id))
        summary2["source"] = "raw_sqlite_detect"
        return {"ok": True, "summary": summary2}
    except Exception as e:
        return {"ok": False, "error": str(e)}


# ==========================================================
# Admin Bulk API (guarded)
# ==========================================================
class BulkActionPayload(BaseModel):
    run_ids: List[int]
    action: str  # "AUDIT" | "FIX" | "AUTO" | "EXPORT"
    max_rounds: int = 2  # FIX/AUTO에서만 사용


def _admin_do_audit(run_id: int, base_url: str) -> Dict[str, Any]:
    run = get_run(DB, run_id)
    primary = run.get("primary_keyword", "")
    supporting = run.get("supporting_keywords", []) or []
    intent = run.get("intent", "구매형")

    page = _make_page_snapshot(run, variant=None, base_url=base_url)
    audit = seo_audit_page(page, primary, supporting, intent)

    run2 = update_run(DB, run_id, fields={"audit": audit, "stage": ST_AUDIT_DONE})
    return {"run": run2, "audit": audit}


def _admin_do_fix(run_id: int, base_url: str, max_rounds: int) -> Dict[str, Any]:
    run = get_run(DB, run_id)
    primary = run.get("primary_keyword", "")
    supporting = run.get("supporting_keywords", []) or []
    intent = run.get("intent", "구매형")

    before_page = _make_page_snapshot(run, variant=None, base_url=base_url)
    before_audit = seo_audit_page(before_page, primary, supporting, intent)

    if before_audit.get("overall") == "PASS":
        run2 = update_run(DB, run_id, fields={"audit": before_audit, "stage": ST_AUDIT_DONE})
        return {"run": run2, "before_audit": before_audit, "audit": before_audit, "fixed": None, "diff": None}

    fixed_page = None
    last_audit = before_audit
    rounds = max(1, min(int(max_rounds or 1), 6))
    page = before_page

    for _ in range(rounds):
        fixed_page = ai_fix_to_pass(
            page=page,
            audit=last_audit,
            primary_keyword=primary,
            supporting_keywords=supporting,
            intent=intent,
            run_id=run_id,
            base_url=base_url,
        )
        fixed_page = _enforce_page_for_pass(fixed_page, primary, supporting, intent)
        last_audit = seo_audit_page(fixed_page, primary, supporting, intent)
        page = fixed_page
        if last_audit.get("overall") == "PASS":
            break

    run2 = update_run(DB, run_id, fields={"fixed": fixed_page, "audit": last_audit, "stage": ST_FIXED})
    diff = _diff_before_after(before_page, fixed_page or {}) if fixed_page else None
    return {"run": run2, "before_audit": before_audit, "audit": last_audit, "fixed": fixed_page, "diff": diff}


def _admin_do_export(run_id: int, base_url: str) -> Dict[str, Any]:
    run = get_run(DB, run_id)
    audit = run.get("audit") or {}
    if audit.get("overall") != "PASS":
        st = audit.get("overall", "UNKNOWN")
        raise ValueError(f"Export blocked: SEO is not PASS (current={st})")

    primary = run.get("primary_keyword", "")
    supporting = run.get("supporting_keywords", []) or []
    intent = run.get("intent", "구매형")

    page = _make_page_snapshot(run, variant=None, base_url=base_url)
    html = _render_export_html(
        page,
        primary_keyword=primary,
        supporting_keywords=supporting,
        intent=intent,
        canonical_fallback=f"{base_url}/r/{run_id}",
    )

    out = EXPORT_DIR / f"run_{run_id}.html"
    out.write_text(html, encoding="utf-8")

    export = {"path": str(out), "ts": _now()}
    run2 = update_run(DB, run_id, fields={"export": export, "stage": ST_EXPORTED})
    return {"run": run2, "export": export}


def _admin_do_auto(run_id: int, base_url: str, max_rounds: int) -> Dict[str, Any]:
    run = get_run(DB, run_id)
    primary = run.get("primary_keyword", "")
    supporting = run.get("supporting_keywords", []) or []
    intent = run.get("intent", "구매형")

    before_page = _make_page_snapshot(run, variant=None, base_url=base_url)
    before_audit = seo_audit_page(before_page, primary, supporting, intent)

    fixed_page = None
    last_audit = before_audit
    rounds = max(1, min(int(max_rounds or 2), 6))
    page = before_page

    if before_audit.get("overall") != "PASS":
        for _ in range(rounds):
            fixed_page = ai_fix_to_pass(
                page=page,
                audit=last_audit,
                primary_keyword=primary,
                supporting_keywords=supporting,
                intent=intent,
                run_id=run_id,
                base_url=base_url,
            )
            fixed_page = _enforce_page_for_pass(fixed_page, primary, supporting, intent)
            last_audit = seo_audit_page(fixed_page, primary, supporting, intent)
            page = fixed_page
            if last_audit.get("overall") == "PASS":
                break

        run = update_run(DB, run_id, fields={"fixed": fixed_page, "audit": last_audit, "stage": ST_FIXED})
    else:
        run = update_run(DB, run_id, fields={"audit": before_audit, "stage": ST_AUDIT_DONE})

    diff = _diff_before_after(before_page, fixed_page or {}) if fixed_page else None

    if (run.get("audit") or {}).get("overall") != "PASS":
        return {
            "run": run,
            "before_audit": before_audit,
            "audit": run.get("audit"),
            "fixed": fixed_page,
            "diff": diff,
            "export": None,
        }

    exp = _admin_do_export(run_id, base_url)
    run2 = exp["run"]
    return {
        "run": run2,
        "before_audit": before_audit,
        "audit": run2.get("audit"),
        "fixed": fixed_page,
        "diff": diff,
        "export": exp.get("export"),
    }


@app.post("/api/admin/bulk_action")
def api_admin_bulk_action(payload: BulkActionPayload, request: Request):
    _admin_guard(request)

    action = (payload.action or "").strip().upper()
    run_ids = [int(x) for x in (payload.run_ids or []) if str(x).strip().isdigit()]
    run_ids = list(dict.fromkeys(run_ids))  # 중복 제거, 순서 유지
    if not run_ids:
        return _json_error(400, "run_ids is empty")

    if action not in ("AUDIT", "FIX", "AUTO", "EXPORT"):
        return _json_error(400, "action must be AUDIT|FIX|AUTO|EXPORT")

    base_url = str(request.base_url).rstrip("/")
    max_rounds = int(payload.max_rounds or 2)

    results: List[Dict[str, Any]] = []
    t0_all = time.perf_counter()

    ok_count = 0
    err_count = 0

    for rid in run_ids:
        t0 = time.perf_counter()
        try:
            if action == "AUDIT":
                out = _admin_do_audit(rid, base_url)
                run = out["run"]
                audit = out.get("audit") or {}
                elapsed = int((time.perf_counter() - t0) * 1000)
                _log_job(rid, "BULK_AUDIT", "OK", elapsed, {"audit": audit})
                results.append(
                    {
                        "run_id": rid,
                        "ok": True,
                        "action": action,
                        "stage": run.get("stage"),
                        "audit_overall": (audit or {}).get("overall"),
                        "score": (audit or {}).get("score"),
                        "export": run.get("export"),
                    }
                )
                ok_count += 1

            elif action == "FIX":
                out = _admin_do_fix(rid, base_url, max_rounds=max_rounds)
                run = out["run"]
                audit = out.get("audit") or {}
                elapsed = int((time.perf_counter() - t0) * 1000)
                _log_job(rid, "BULK_FIX", "OK", elapsed, {"audit": audit, "diff": out.get("diff")})
                results.append(
                    {
                        "run_id": rid,
                        "ok": True,
                        "action": action,
                        "stage": run.get("stage"),
                        "audit_overall": (audit or {}).get("overall"),
                        "score": (audit or {}).get("score"),
                        "export": run.get("export"),
                    }
                )
                ok_count += 1

            elif action == "AUTO":
                out = _admin_do_auto(rid, base_url, max_rounds=max_rounds)
                run = out["run"]
                audit = out.get("audit") or {}
                elapsed = int((time.perf_counter() - t0) * 1000)
                status = "OK" if (audit or {}).get("overall") == "PASS" and run.get("export") else "WARN"
                _log_job(
                    rid,
                    "BULK_AUTO",
                    status,
                    elapsed,
                    {"audit": audit, "diff": out.get("diff"), "export": out.get("export")},
                )
                results.append(
                    {
                        "run_id": rid,
                        "ok": True,
                        "action": action,
                        "stage": run.get("stage"),
                        "audit_overall": (audit or {}).get("overall"),
                        "score": (audit or {}).get("score"),
                        "export": run.get("export"),
                    }
                )
                ok_count += 1

            else:  # EXPORT
                out = _admin_do_export(rid, base_url)
                run = out["run"]
                elapsed = int((time.perf_counter() - t0) * 1000)
                _log_job(rid, "BULK_EXPORT", "OK", elapsed, {"export": out.get("export")})
                results.append(
                    {
                        "run_id": rid,
                        "ok": True,
                        "action": action,
                        "stage": run.get("stage"),
                        "audit_overall": (run.get("audit") or {}).get("overall"),
                        "score": (run.get("audit") or {}).get("score"),
                        "export": run.get("export"),
                    }
                )
                ok_count += 1

        except Exception as e:
            elapsed = int((time.perf_counter() - t0) * 1000)
            _log_job(rid, f"BULK_{action}", "ERR", elapsed, {"error": str(e)})
            results.append({"run_id": rid, "ok": False, "action": action, "error": str(e)})
            err_count += 1

    elapsed_all = int((time.perf_counter() - t0_all) * 1000)
    return {
        "ok": True,
        "action": action,
        "requested": len(run_ids),
        "ok_count": ok_count,
        "err_count": err_count,
        "elapsed_ms": elapsed_all,
        "results": results,
    }
