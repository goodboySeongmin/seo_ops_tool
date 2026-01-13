from __future__ import annotations

import json
import os
from typing import Any, Dict, List, Optional


def _env_bool(name: str, default: bool = False) -> bool:
    v = (os.getenv(name) or "").strip().lower()
    if not v:
        return default
    return v in ("1", "true", "yes", "y", "on")


def _clip(s: str, n: int) -> str:
    s = (s or "").strip()
    return s if len(s) <= n else s[: n - 1].rstrip() + "…"


def _build_prompt(
    *,
    page: Dict[str, Any],
    audit: Dict[str, Any],
    primary_keyword: str,
    supporting_keywords: List[str],
    intent: str,
) -> str:
    issues = audit.get("issues") or []
    issues_txt = "\n".join(
        [
            f"- {i.get('rule_id','?')} {i.get('severity','?')}: {i.get('message','')}"
            for i in issues
            if isinstance(i, dict)
        ]
    )

    meta_title = (page.get("meta_title") or "").strip()
    meta_desc = (page.get("meta_description") or "").strip()

    # LLM에게 “meta만” 바꾸라고 강하게 제한
    return f"""
너는 한국어 SEO 랜딩페이지 편집자다.
아래 입력을 바탕으로 meta_title, meta_description만 최소 수정으로 개선해라.
다른 필드(h1/body/cta/faq)는 절대 건드리지 않는다.

목표:
- meta_title 길이: 30~60자 권장 (너무 짧거나 길면 조정)
- meta_description 길이: 70~160자 권장
- meta_title/meta_description에 primary keyword를 자연스럽게 포함(과도 반복 금지)
- 과장/효능 단정/의학적 확정 표현 금지(뷰티/화장품 표현 안전하게)
- 문장 자연스럽고 클릭 유도는 하되 “사실 기반 톤” 유지
- supporting keywords는 가능하면 description에 1~2개만 자연스럽게(억지 금지)
- 기존 문맥/의도({intent}) 유지

현재 primary keyword: {primary_keyword}
supporting keywords: {", ".join(supporting_keywords)}

현재 meta_title:
{meta_title}

현재 meta_description:
{meta_desc}

SEO audit 이슈:
{issues_txt if issues_txt else "- (이슈 요약 없음)"}

반드시 JSON만 출력해라. 형식:
{{
  "meta_title": "...",
  "meta_description": "..."
}}
""".strip()


def llm_fix_patch(
    *,
    page: Dict[str, Any],
    audit: Dict[str, Any],
    primary_keyword: str,
    supporting_keywords: List[str],
    intent: str,
) -> Dict[str, str]:
    """
    LLM로 meta_title/meta_description만 '패치' 제안.
    - 실패/비활성/파싱 실패 시 {} 반환
    - OPENAI_API_KEY 필요
    - 모델: SEO_LLM_MODEL (default: gpt-4o-mini)
    """
    if not _env_bool("SEO_LLM_FIX", default=False):
        return {}

    api_key = (os.getenv("OPENAI_API_KEY") or "").strip()
    if not api_key:
        return {}

    model = (os.getenv("SEO_LLM_MODEL") or "gpt-4o-mini").strip()

    prompt = _build_prompt(
        page=page,
        audit=audit or {},
        primary_keyword=primary_keyword,
        supporting_keywords=supporting_keywords or [],
        intent=intent or "구매형",
    )

    # ---- OpenAI client: responses API 우선, 없으면 chat.completions fallback ----
    try:
        from openai import OpenAI  # type: ignore
    except Exception:
        return {}

    client = OpenAI(api_key=api_key)

    text = ""
    # 1) responses API
    try:
        resp = client.responses.create(
            model=model,
            input=prompt,
            # 가능한 경우 json 유도 (라이브러리/모델에 따라 무시될 수 있음)
            text={"format": {"type": "json_object"}},
        )
        # resp.output_text 가 있으면 사용
        text = getattr(resp, "output_text", "") or ""
    except Exception:
        text = ""

    # 2) chat.completions fallback
    if not text:
        try:
            cc = client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": "You output ONLY valid JSON."},
                    {"role": "user", "content": prompt},
                ],
                temperature=0.2,
            )
            text = (cc.choices[0].message.content or "").strip()
        except Exception:
            return {}

    # ---- parse JSON ----
    try:
        # 모델이 앞뒤에 설명을 붙였을 때 대비: 첫 { ... } 블록만 뽑기
        s = text.strip()
        if "{" in s and "}" in s:
            s = s[s.find("{") : s.rfind("}") + 1]
        obj = json.loads(s)
    except Exception:
        return {}

    meta_title = (obj.get("meta_title") or "").strip()
    meta_desc = (obj.get("meta_description") or "").strip()

    if not meta_title and not meta_desc:
        return {}

    # 안전한 길이 클립(극단 방지)
    meta_title = _clip(meta_title, 80)
    meta_desc = _clip(meta_desc, 220)

    return {
        "meta_title": meta_title,
        "meta_description": meta_desc,
    }
