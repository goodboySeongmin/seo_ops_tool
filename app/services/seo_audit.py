from __future__ import annotations

import json
import math
import re
from typing import Any, Dict, List, Optional


TAG_RE = re.compile(r"<[^>]+>")
H1_RE = re.compile(r"<h1\b[^>]*>(.*?)</h1>", re.IGNORECASE | re.DOTALL)
H2_RE = re.compile(r"<h2\b[^>]*>(.*?)</h2>", re.IGNORECASE | re.DOTALL)


def _strip_tags(html: str) -> str:
    return TAG_RE.sub(" ", html or "")


def _norm(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip().lower()


def _word_count(text: str) -> int:
    t = re.sub(r"\s+", " ", (text or "").strip())
    if not t:
        return 0
    return len(t.split(" "))


def build_faq_jsonld(faq: List[Dict[str, str]]) -> Dict[str, Any]:
    # FAQPage JSON-LD 최소 유효 형태
    main = []
    for item in faq or []:
        q = (item.get("q") or "").strip()
        a = (item.get("a") or "").strip()
        if not q or not a:
            continue
        main.append({
            "@type": "Question",
            "name": q,
            "acceptedAnswer": {"@type": "Answer", "text": a}
        })
    return {
        "@context": "https://schema.org",
        "@type": "FAQPage",
        "mainEntity": main
    }


def seo_audit_page(page: Dict[str, Any], primary_keyword: str, supporting_keywords: List[str], intent: str) -> Dict[str, Any]:
    """
    Google SEO Audit v1.0 (스킨케어 랜딩 기준)
    page: {meta_title, meta_description, canonical_url, og_title, og_description, h1, body_html, faq, has_faq_jsonld}
    """
    meta_title = (page.get("meta_title") or "").strip()
    meta_desc = (page.get("meta_description") or "").strip()
    canonical = (page.get("canonical_url") or "").strip()
    og_title = (page.get("og_title") or "").strip()
    og_desc = (page.get("og_description") or "").strip()

    h1 = (page.get("h1") or "").strip()
    body_html = page.get("body_html") or ""
    body_text = _strip_tags(body_html)
    faq = page.get("faq") or []
    has_faq_jsonld = bool(page.get("has_faq_jsonld")) or (len(faq) >= 3)

    primary = (primary_keyword or "").strip()
    supporting = [s.strip() for s in (supporting_keywords or []) if s.strip()]

    # signals
    h1_count = len(H1_RE.findall(body_html)) if body_html else 0
    h2_count = len(H2_RE.findall(body_html)) if body_html else 0

    # fallback: body_html에 h1/h2가 없다면, 'h1' 필드만으로 카운트
    if h1 and h1_count == 0:
        h1_count = 1

    title_len = len(meta_title)
    desc_len = len(meta_desc)

    words = _word_count(body_text)
    faq_count = len([x for x in faq if (x.get("q") or "").strip() and (x.get("a") or "").strip()])

    norm_primary = _norm(primary)
    norm_h1 = _norm(h1)
    norm_body = _norm(body_text)

    primary_kw_in_h1 = bool(norm_primary) and (norm_primary in norm_h1)
    # 첫 120단어 내 포함
    first120 = " ".join((body_text or "").split(" ")[:120])
    primary_kw_in_first120 = bool(norm_primary) and (norm_primary in _norm(first120))

    supporting_hits = 0
    for sk in supporting:
        if _norm(sk) and (_norm(sk) in norm_body):
            supporting_hits += 1

    # keyword density (매우 러프하게)
    primary_count = 0
    if norm_primary:
        primary_count = len(re.findall(re.escape(norm_primary), norm_body))
    density = 0.0
    if words > 0:
        density = (primary_count / max(words, 1)) * 100.0

    issues: List[Dict[str, Any]] = []
    penalty = 0

    def add(rule_id: str, severity: str, message: str, fix_hint: str, p: int):
        nonlocal penalty
        issues.append({
            "rule_id": rule_id,
            "severity": severity,
            "message": message,
            "fix_hint": fix_hint
        })
        penalty += p

    # ---- T rules ----
    if not meta_title:
        add("T001", "FAIL", "Meta title이 비어 있습니다.", "primary keyword를 포함한 35~55자 title을 생성하세요.", 25)
    else:
        if not (30 <= title_len <= 60):
            add("T002", "WARN", f"Meta title 길이가 권장 범위(30~60자)가 아닙니다. (len={title_len})",
                "너무 짧으면 USP+키워드 보강, 너무 길면 군더더기 제거.", 10)

    if not meta_desc:
        add("T003", "FAIL", "Meta description이 비어 있습니다.", "90~150자 내로 혜택/차별점/CTA를 과장 없이 작성.", 25)
    else:
        if not (70 <= desc_len <= 160):
            add("T004", "WARN", f"Meta description 길이가 권장 범위(70~160자)가 아닙니다. (len={desc_len})",
                "너무 짧으면 설명 보강, 너무 길면 핵심만 남기기.", 10)

    if not canonical:
        add("T005", "WARN", "Canonical이 없습니다.", "배포 URL이 정해지면 canonical을 고정하세요(placeholder라도 추가).", 10)

    if h1_count != 1:
        add("T006", "FAIL", f"H1 개수가 1개가 아닙니다. (count={h1_count})",
            "H1을 1개로 통합하세요(가장 중요한 메시지 + primary keyword).", 25)

    if not og_title or not og_desc:
        # INFO 수준
        add("T007", "INFO", "OpenGraph 태그가 부족합니다(og:title/og:description).", "meta title/desc를 재사용해 OG를 채우세요.", 2)

    # ---- C rules ----
    if primary and not primary_kw_in_h1:
        add("C001", "FAIL", "H1에 primary keyword가 포함되지 않습니다.",
            "H1에 primary keyword를 자연스럽게 포함하도록 수정하세요.", 25)

    if primary and not primary_kw_in_first120:
        add("C002", "WARN", "본문 초반(120단어 내)에 primary keyword가 없습니다.",
            "첫 단락을 리라이트해 primary keyword를 1회 포함시키세요.", 10)

    if supporting and supporting_hits < 2:
        add("C003", "WARN", f"Supporting keyword 커버리지가 부족합니다. (hits={supporting_hits})",
            "섹션별로 supporting keyword를 2개 이상 자연스럽게 포함시키세요.", 10)

    if h2_count < 3:
        add("C004", "FAIL", f"H2 섹션 수가 부족합니다. (h2={h2_count})",
            f"intent({intent})에 맞는 H2 3~5개 섹션을 생성하세요(성분/사용법/루틴/FAQ 등).", 25)

    if words < 350:
        add("C005", "WARN", f"본문이 얇습니다(단어 수 {words}).", "섹션을 확장해 350~600단어 수준으로 보강하세요.", 10)

    # 키워드 스팸(아주 러프한 기준)
    if primary and density > 3.0:
        add("C007", "FAIL", f"Primary keyword 반복이 과다합니다(density≈{density:.2f}%).",
            "동의어/문장 재구성으로 반복을 낮추세요(0.5~2.5% 권장).", 25)

    # ---- E rules (뷰티 운영 리스크) ----
    banned = [
        "완치", "치료", "즉시", "무조건", "100%", "부작용 없음", "의사 추천", "염증 치료",
        "아토피 치료", "피부병 치료"
    ]
    for w in banned:
        if w and (w in body_text or w in meta_title or w in meta_desc):
            # 의약품 오인/치료는 FAIL
            add("E002", "FAIL", f"금칙/오인 표현 감지: '{w}'", "효능 단정/치료 표현을 삭제하거나 완화 표현으로 변경.", 25)
            break

    disclaimer_candidates = ["개인차", "피부 상태", "패치 테스트", "민감한 경우"]
    has_disclaimer = any(x in body_text for x in disclaimer_candidates)
    if not has_disclaimer:
        add("E001", "WARN", "개인차/주의 문구가 부족합니다.", "하단에 개인차/패치 테스트 등 주의 문구 1줄 추가.", 10)

    # ---- S rules ----
    if faq_count < 3:
        add("S001", "WARN", f"FAQ가 부족합니다. (faq_count={faq_count})", "FAQ 3~5개를 추가하세요.", 10)

    if faq_count >= 3 and not has_faq_jsonld:
        add("S002", "WARN", "FAQ JSON-LD가 없습니다.", "FAQPage JSON-LD를 생성해 <script type='application/ld+json'>로 삽입.", 10)

    # score / overall
    score = max(0, 100 - penalty)

    overall = "PASS"
    if any(i["severity"] == "FAIL" for i in issues):
        overall = "FAIL"
    else:
        warn_cnt = sum(1 for i in issues if i["severity"] == "WARN")
        if warn_cnt >= 2:
            overall = "WARN"

    return {
        "overall": overall,
        "score": score,
        "issues": issues,
        "signals": {
            "title_len": title_len,
            "desc_len": desc_len,
            "h1_count": h1_count,
            "h2_count": h2_count,
            "word_count": words,
            "primary_kw_in_h1": primary_kw_in_h1,
            "primary_kw_in_first120": primary_kw_in_first120,
            "supporting_kw_hits": supporting_hits,
            "faq_count": faq_count,
            "has_faq_jsonld": has_faq_jsonld,
            "primary_kw_density_pct": round(density, 4)
        }
    }
