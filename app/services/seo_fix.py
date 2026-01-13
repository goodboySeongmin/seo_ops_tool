# services/seo_fix.py
from __future__ import annotations

import json
import os
import re
from typing import Any, Dict, List, Optional

_TAG_RE = re.compile(r"<[^>]+>")
_WS_RE = re.compile(r"\s+")
_H2_RE = re.compile(r"<h2\b", re.IGNORECASE)


def strip_tags(html: str) -> str:
    return _WS_RE.sub(" ", _TAG_RE.sub(" ", html or "")).strip()


def word_count(text: str) -> int:
    tokens = re.findall(r"\S+", text or "")
    return len(tokens)


def count_h2(body_html: str) -> int:
    return len(_H2_RE.findall(body_html or ""))


def ensure_nonempty(s: Optional[str], fallback: str) -> str:
    s = (s or "").strip()
    return s if s else fallback


def clamp_text_len(s: str, lo: int, hi: int, pad: str) -> str:
    """길이를 권장 범위로 억지로 맞춤(짧으면 pad 붙이고, 길면 자름)."""
    s = (s or "").strip()
    if not s:
        s = pad
    if len(s) < lo:
        need = lo - len(s)
        s = (s + " " + pad)[: lo + max(0, need)]
    if len(s) > hi:
        s = s[:hi].rstrip()
    return s


def safe_primary(pack: Dict[str, Any]) -> str:
    v = (pack.get("primary_keyword") or "").strip()
    if v:
        return v
    v = (pack.get("meta_title") or "").strip()
    return v.split("|")[0].strip() if v else "랜딩 페이지"


def parse_supporting(supporting_keywords: Any) -> List[str]:
    if isinstance(supporting_keywords, list):
        return [str(x).strip() for x in supporting_keywords if str(x).strip()]
    if isinstance(supporting_keywords, str):
        return [x.strip() for x in supporting_keywords.split(",") if x.strip()]
    return []


def ensure_primary_in_intro(body_html: str, primary_kw: str) -> str:
    """첫 120 words 안에 primary가 없으면 intro 문단을 맨 앞에 추가."""
    plain = strip_tags(body_html)
    first_120 = " ".join(plain.split()[:120])
    if primary_kw and primary_kw not in first_120:
        intro = (
            f"<p><b>{primary_kw}</b>를 찾는 분들을 위해, 이 페이지는 선택 기준/사용 루틴/FAQ를 "
            f"사실 기반으로 정리했습니다.</p>"
        )
        return intro + "\n" + (body_html or "")
    return body_html


def ensure_disclaimer(body_html: str) -> str:
    """T006 경고 제거용 문구(과장/의학적 표현 피함)."""
    plain = strip_tags(body_html)
    if "개인차" in plain or "전문가" in plain or "이상 반응" in plain:
        return body_html
    block = (
        "<p><i>※ 안내: 본 내용은 일반 정보이며 개인 피부 상태에 따라 반응이 다를 수 있습니다. "
        "사용 중 이상 반응이 있으면 사용을 중단하고 전문가와 상담하세요.</i></p>"
    )
    return (body_html or "") + "\n" + block


def ensure_supporting_hits(body_html: str, supporting: List[str], min_hits: int = 2) -> str:
    """
    supporting keyword 히트가 부족하면 본문에 자연스럽게 삽입.
    최소 2개는 박아 넣는다.
    """
    supporting = [s for s in (supporting or []) if s]
    if not supporting:
        return body_html

    plain = strip_tags(body_html).lower()
    hits = sum(1 for s in supporting if s.lower() in plain)

    if hits >= min_hits:
        return body_html

    need = min_hits - hits
    insert = supporting[: max(2, need + 1)]
    line = (
        "<p>관련 키워드 기준으로는 "
        + ", ".join([f"<b>{x}</b>" for x in insert])
        + " 관점에서 함께 살펴보는 것이 도움이 됩니다.</p>"
    )
    return (body_html or "") + "\n" + line


def reduce_keyword_density(body_html: str, primary_kw: str, max_density: float = 0.03) -> str:
    """
    키워드 밀도가 높으면 일부 반복을 약화.
    - 너무 공격적으로 바꾸면 의미가 깨지니, 4번째 등장부터 격번 치환.
    """
    if not primary_kw:
        return body_html

    plain = strip_tags(body_html)
    wc = word_count(plain)
    if wc <= 0:
        return body_html

    occ = plain.count(primary_kw)
    density = occ / max(wc, 1)

    if density <= max_density:
        return body_html

    replaced = []
    idx = 0
    parts = (body_html or "").split(primary_kw)
    if len(parts) <= 1:
        return body_html

    out = parts[0]
    for i in range(1, len(parts)):
        idx += 1
        token = primary_kw
        if idx >= 4 and idx % 2 == 0:
            token = "해당 제품"
        out += token + parts[i]
    return out


def ensure_h1(fixed: Dict[str, Any], primary_kw: str) -> None:
    fixed["h1"] = ensure_nonempty(fixed.get("h1"), primary_kw)


def ensure_h2_sections(fixed: Dict[str, Any], primary_kw: str, intent: str) -> None:
    body = fixed.get("body_html") or ""
    current = count_h2(body)
    min_h2 = 3 if (intent or "").strip() == "구매형" else 2
    if current >= min_h2:
        return

    sections: List[str] = []
    sections.append(
        f"<h2>{primary_kw} 핵심 포인트</h2>"
        f"<p>{primary_kw}를 고를 때는 보습 지속감, 사용감, 성분 구성, 개인 피부 컨디션을 함께 고려하는 것이 좋습니다.</p>"
    )
    sections.append(
        "<h2>성분/구성 체크</h2>"
        "<p>세라마이드, 판테놀 등은 보습/진정 루틴에서 자주 언급됩니다. 다만 개인차가 있으니 패치 테스트를 권장합니다.</p>"
    )
    sections.append(
        "<h2>사용 루틴</h2>"
        "<p>세안 후 토너 다음 단계에서 적당량을 얇게 펴 바르고, 건조한 부위는 소량을 레이어링하세요. 낮에는 자외선 차단제로 마무리하세요.</p>"
    )
    if (intent or "").strip() == "구매형":
        sections.append(
            "<h2>구매 전 확인</h2>"
            "<ul>"
            "<li>피부 타입/민감도와의 적합성(패치 테스트 권장)</li>"
            "<li>제형/사용감(계절/습도에 따라 체감 차이)</li>"
            "<li>전성분 및 알레르기 유발 가능 성분 확인</li>"
            "</ul>"
        )

    needed = max(0, min_h2 - current)
    if needed > 0:
        fixed["body_html"] = body + "\n" + "\n".join(sections[:needed])


def ensure_faq(fixed: Dict[str, Any], primary_kw: str, min_faq: int = 3) -> None:
    faq = fixed.get("faq") or []
    if not isinstance(faq, list):
        faq = []

    existing_q = {str(item.get("q", "")).strip() for item in faq if isinstance(item, dict)}
    candidates = [
        (f"{primary_kw}는 민감 피부도 사용 가능한가요?", "개인차가 있으므로 사용 전 패치 테스트를 권장합니다."),
        ("아침/저녁 모두 사용해도 되나요?", "일반적인 보습 루틴에서 아침/저녁 사용이 가능하며 피부 상태에 따라 사용량을 조절하세요."),
        ("어떤 순서로 바르면 좋나요?", "세안→토너→(에센스/세럼)→크림 순으로 마무리하는 방식이 일반적입니다."),
        ("향이나 제형이 궁금해요.", "제품별로 상이하므로 상세 페이지의 표기 정보를 확인하는 것을 권장합니다."),
    ]

    for q, a in candidates:
        if len(faq) >= min_faq:
            break
        if q in existing_q:
            continue
        faq.append({"q": q, "a": a})
        existing_q.add(q)

    fixed["faq"] = faq
    fixed["has_faq_jsonld"] = True


def ensure_cta(fixed: Dict[str, Any], intent: str) -> None:
    if (fixed.get("cta") or "").strip():
        return
    fixed["cta"] = "지금 구매하기" if (intent or "").strip() == "구매형" else "자세히 보기"


def ensure_body_length(fixed: Dict[str, Any], primary_kw: str, min_words: int = 360) -> None:
    body = fixed.get("body_html") or ""
    plain = strip_tags(body)
    wc = word_count(plain)
    if wc >= min_words:
        return

    pad = (
        f"<h2>{primary_kw} 선택 기준</h2>"
        f"<p>{primary_kw}를 선택할 때는 보습 지속감, 자극 가능성(개인차), 사용감(흡수/잔여감), 계절/습도 환경을 함께 고려하는 것이 좋습니다.</p>"
        "<h2>사용 팁</h2>"
        "<p>크림은 적당량을 여러 번 레이어링하는 방식이 더 편안할 수 있습니다. 건조한 부위는 소량을 한 번 더 덧바르세요.</p>"
        "<h2>주의 사항</h2>"
        "<p>사용 중 붉어짐/가려움/따가움 등 이상 반응이 있으면 사용을 중단하고 전문가와 상담하세요.</p>"
    )

    loops = 0
    while wc < min_words and loops < 3:
        body += "\n" + pad
        plain = strip_tags(body)
        wc = word_count(plain)
        loops += 1

    fixed["body_html"] = body


def ensure_canonical(fixed: Dict[str, Any], run_id: int, base_url: str) -> None:
    canonical = (fixed.get("canonical_url") or "").strip()
    if canonical:
        return
    base = (base_url or "https://example.com").rstrip("/")
    fixed["canonical_url"] = f"{base}/r/{run_id}"


def ensure_og(fixed: Dict[str, Any]) -> None:
    fixed["og_title"] = ensure_nonempty(fixed.get("og_title"), fixed.get("meta_title") or "")
    fixed["og_description"] = ensure_nonempty(
        fixed.get("og_description"),
        fixed.get("meta_description") or "",
    )


# --------------------------
# Hybrid helpers
# --------------------------
def _likely_pass_quickcheck(page: Dict[str, Any], primary_kw: str, supporting: List[str], intent: str) -> bool:
    """LLM 호출 전에 '대충 PASS일 가능성'을 빠르게 체크해서 비용/속도 절약."""
    mt = (page.get("meta_title") or "").strip()
    md = (page.get("meta_description") or "").strip()
    h1 = (page.get("h1") or "").strip()
    canon = (page.get("canonical_url") or "").strip()
    body = (page.get("body_html") or "").strip()
    faq = page.get("faq") or []

    if not (20 <= len(mt) <= 60):
        return False
    if not (60 <= len(md) <= 170):
        return False
    if not h1:
        return False
    if not canon:
        return False
    if count_h2(body) < (3 if (intent or "").strip() == "구매형" else 2):
        return False
    if word_count(strip_tags(body)) < 360:
        return False
    if not isinstance(faq, list) or len(faq) < 3:
        return False

    # primary intro 포함
    plain = strip_tags(body)
    first_120 = " ".join(plain.split()[:120])
    if primary_kw and primary_kw not in first_120:
        return False

    # supporting 최소 2개 히트
    sup = [s for s in (supporting or []) if s]
    if sup:
        low = plain.lower()
        hits = sum(1 for s in sup if s.lower() in low)
        if hits < 2:
            return False

    # 키워드 과밀도(대충)
    wc = word_count(plain)
    if wc > 0 and primary_kw:
        occ = plain.count(primary_kw)
        if (occ / wc) > 0.035:
            return False

    # disclaimer 포함
    if "개인차" not in plain or "이상 반응" not in plain:
        return False

    return True


def _rule_fix(
    *,
    page: Dict[str, Any],
    primary_kw: str,
    supporting: List[str],
    intent: str,
    run_id: int | None,
    base_url: str | None,
) -> Dict[str, Any]:
    fixed: Dict[str, Any] = {
        "meta_title": (page.get("meta_title") or "").strip(),
        "meta_description": (page.get("meta_description") or "").strip(),
        "canonical_url": (page.get("canonical_url") or "").strip(),
        "og_title": (page.get("og_title") or "").strip(),
        "og_description": (page.get("og_description") or "").strip(),
        "h1": (page.get("h1") or "").strip(),
        "body_html": (page.get("body_html") or "").strip(),
        "cta": (page.get("cta") or "").strip(),
        "faq": page.get("faq") or [],
        "has_faq_jsonld": bool(page.get("has_faq_jsonld")),
    }

    fixed["meta_title"] = clamp_text_len(
        fixed.get("meta_title", ""),
        lo=20,
        hi=60,
        pad=f"{primary_kw} 가이드 | 선택 기준",
    )
    fixed["meta_description"] = clamp_text_len(
        fixed.get("meta_description", ""),
        lo=60,
        hi=170,
        pad=f"{primary_kw} 선택 기준, 사용 루틴, FAQ를 정리했습니다. 개인차가 있어 패치 테스트를 권장합니다.",
    )

    ensure_h1(fixed, primary_kw)
    ensure_cta(fixed, intent)
    ensure_h2_sections(fixed, primary_kw, intent)
    ensure_faq(fixed, primary_kw, min_faq=3)
    ensure_body_length(fixed, primary_kw, min_words=360)

    fixed["body_html"] = ensure_primary_in_intro(fixed.get("body_html") or "", primary_kw)
    fixed["body_html"] = ensure_supporting_hits(fixed.get("body_html") or "", supporting, min_hits=2)
    fixed["body_html"] = ensure_disclaimer(fixed.get("body_html") or "")
    fixed["body_html"] = reduce_keyword_density(fixed.get("body_html") or "", primary_kw, max_density=0.03)

    if run_id is not None and base_url is not None:
        ensure_canonical(fixed, run_id, base_url)
    ensure_og(fixed)

    return fixed


def _extract_json_maybe(text: str) -> Optional[Dict[str, Any]]:
    if not text:
        return None
    t = text.strip()

    # ```json ... ``` 제거
    if t.startswith("```"):
        t = re.sub(r"^```[a-zA-Z]*\s*", "", t)
        t = re.sub(r"\s*```$", "", t)

    # 가장 바깥 {}만 잡아보기
    m = re.search(r"\{.*\}", t, flags=re.DOTALL)
    if m:
        t = m.group(0)

    try:
        obj = json.loads(t)
        if isinstance(obj, dict):
            return obj
    except Exception:
        return None
    return None


def _llm_fix(
    *,
    page: Dict[str, Any],
    audit: Dict[str, Any],
    primary_kw: str,
    supporting: List[str],
    intent: str,
) -> Optional[Dict[str, Any]]:
    """
    LLM로 최종 보정(룰로 안 잡히는 WARN/FAIL을 정리).
    - 실패하면 None 반환 (시스템은 룰 결과로 계속 진행)
    """
    if not (os.getenv("OPENAI_API_KEY") or "").strip():
        return None

    model = (os.getenv("SEO_FIX_MODEL") or os.getenv("OPENAI_MODEL") or "gpt-4o-mini").strip()
    temperature = float(os.getenv("SEO_FIX_TEMP") or "0.2")

    sys = (
        "You are a senior SEO editor for Korean landing pages. "
        "Return ONLY valid JSON. No markdown. No extra text."
    )
    user = {
        "task": "Fix the landing page content to pass SEO audit while staying factual and non-medical.",
        "constraints": {
            "language": "ko",
            "meta_title_length": "20-60",
            "meta_description_length": "60-170",
            "min_body_words": 360,
            "min_h2_sections": 3 if (intent or "").strip() == "구매형" else 2,
            "min_faq": 3,
            "must_include_disclaimer": True,
            "primary_in_first_120_words": True,
            "supporting_keywords_min_hits": 2,
            "keyword_density_max": 0.03,
            "avoid_medical_claims": True,
        },
        "inputs": {
            "primary_keyword": primary_kw,
            "supporting_keywords": supporting,
            "intent": intent,
            "current_page": page,
            "audit": audit,
        },
        "output_schema": {
            "meta_title": "string",
            "meta_description": "string",
            "h1": "string",
            "body_html": "string (HTML allowed)",
            "cta": "string",
            "faq": [{"q": "string", "a": "string"}],
        },
    }

    # SDK 우선 사용, 실패하면 룰로 계속
    try:
        from openai import OpenAI  # type: ignore
        client = OpenAI()

        # Responses API가 있으면 사용, 없으면 ChatCompletions
        if hasattr(client, "responses"):
            resp = client.responses.create(
                model=model,
                input=[
                    {"role": "system", "content": sys},
                    {"role": "user", "content": json.dumps(user, ensure_ascii=False)},
                ],
                temperature=temperature,
            )
            # 다양한 형태 대응: output_text 우선
            text = ""
            if hasattr(resp, "output_text") and resp.output_text:
                text = resp.output_text
            else:
                # 최대한 일반적으로 합치기
                try:
                    for o in (resp.output or []):
                        for c in (getattr(o, "content", None) or []):
                            if getattr(c, "type", "") in ("output_text", "text"):
                                text += getattr(c, "text", "") or ""
                except Exception:
                    pass

            obj = _extract_json_maybe(text)
            return obj

        # Chat Completions
        resp2 = client.chat.completions.create(
            model=model,
            temperature=temperature,
            messages=[
                {"role": "system", "content": sys},
                {"role": "user", "content": json.dumps(user, ensure_ascii=False)},
            ],
        )
        text2 = (resp2.choices[0].message.content or "") if resp2 and resp2.choices else ""
        return _extract_json_maybe(text2)

    except Exception:
        return None


def _merge_llm_into_page(base: Dict[str, Any], llm: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(base)

    for k in ["meta_title", "meta_description", "h1", "body_html", "cta"]:
        if isinstance(llm.get(k), str) and llm.get(k).strip():
            out[k] = llm[k].strip()

    if isinstance(llm.get("faq"), list):
        faq_ok = []
        for item in llm["faq"]:
            if not isinstance(item, dict):
                continue
            q = str(item.get("q", "")).strip()
            a = str(item.get("a", "")).strip()
            if q and a:
                faq_ok.append({"q": q, "a": a})
        if faq_ok:
            out["faq"] = faq_ok
            out["has_faq_jsonld"] = len(faq_ok) >= 3

    return out


def ai_fix_to_pass(
    *,
    page: Dict[str, Any],
    audit: Dict[str, Any],
    primary_keyword: str,
    supporting_keywords: List[str] | None = None,
    intent: str = "구매형",
    run_id: int | None = None,
    base_url: str | None = None,
) -> Dict[str, Any]:
    """
    ✅ Hybrid Fix:
    1) 룰 기반으로 빠르게 1차 보정
    2) quickcheck에서 PASS 가능성이 낮으면(=WARN/FAIL 가능) LLM로 최종 보정 시도
    3) LLM 실패/미설정이면 룰 결과 반환

    env:
      - OPENAI_API_KEY: 설정 시 LLM 보정 가능
      - SEO_FIX_MODEL: LLM 모델명 (기본: gpt-4o-mini)
      - SEO_FIX_USE_LLM: 0/false면 LLM 비활성화
    """
    primary_kw = (primary_keyword or "").strip() or safe_primary(page)
    supporting = parse_supporting(supporting_keywords)

    fixed = _rule_fix(
        page=page,
        primary_kw=primary_kw,
        supporting=supporting,
        intent=intent,
        run_id=run_id,
        base_url=base_url,
    )

    use_llm = (os.getenv("SEO_FIX_USE_LLM") or "1").strip().lower() not in ("0", "false", "no", "off")
    if not use_llm:
        return fixed

    # 룰로 이미 PASS 가능성이 높으면 LLM 호출 스킵(속도/비용 절약)
    if _likely_pass_quickcheck(fixed, primary_kw, supporting, intent):
        return fixed

    llm_out = _llm_fix(page=fixed, audit=audit or {}, primary_kw=primary_kw, supporting=supporting, intent=intent)
    if not llm_out:
        return fixed

    merged = _merge_llm_into_page(fixed, llm_out)

    # LLM이 망쳐도 마지막에 룰로 안전장치 한 번 더
    merged = _rule_fix(
        page=merged,
        primary_kw=primary_kw,
        supporting=supporting,
        intent=intent,
        run_id=run_id,
        base_url=base_url,
    )
    return merged
