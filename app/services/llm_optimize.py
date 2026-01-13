from __future__ import annotations

import json
import os
import re
from typing import Any, Dict, List

from openai import OpenAI


def _client() -> OpenAI:
    return OpenAI()


def optimize_ab_pack(
    primary_keyword: str,
    supporting_keywords: List[str],
    intent: str,
    meta_title: str,
    meta_description: str,
    landing_text: str,
) -> Dict[str, Any]:
    pk = (primary_keyword or "").strip()
    sk = [s.strip() for s in (supporting_keywords or []) if s.strip()]
    model = os.getenv("OPENAI_MODEL", "gpt-4.1-mini")

    schema = {
        "variants": {
            "A": {
                "meta_title": "string",
                "meta_description": "string",
                "hero_headline": "string",
                "hero_sub": "string",
                "cta": "string",
                "faq": [{"q": "string", "a": "string"}]
            },
            "B": {
                "meta_title": "string",
                "meta_description": "string",
                "hero_headline": "string",
                "hero_sub": "string",
                "cta": "string",
                "faq": [{"q": "string", "a": "string"}]
            }
        },
        "notes": ["string", "string"]
    }

    prompt = f"""
너는 Google 검색 결과(메타) CTR을 올리기 위한 A/B 카피 최적화 에이전트다.
단, 화장품/뷰티 문구는 과장/치료 단정/의약품 오인 표현을 금지한다.

[컨텍스트]
- intent: {intent}
- primary keyword: {pk}
- supporting keywords: {", ".join(sk)}
- 기존 meta_title: {meta_title}
- 기존 meta_description: {meta_description}
- 랜딩 본문 요약 재료(발췌): {landing_text[:1200]}

[요구]
- Variant A/B 두 개 생성
- meta_title 30~60자, meta_description 70~160자
- hero_headline은 H1에 해당(키워드 포함 권장)
- FAQ는 3~5개(실제 고객 질문처럼)
- 출력은 JSON 하나만 (추가 텍스트 금지)

[출력 스키마 예시]
{json.dumps(schema, ensure_ascii=False)}
""".strip()

    client = _client()
    resp = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": "You output strictly valid JSON only."},
            {"role": "user", "content": prompt},
        ],
        temperature=0.6,
    )
    raw = resp.choices[0].message.content.strip()
    m = re.search(r"\{.*\}", raw, re.DOTALL)
    if m:
        raw = m.group(0)
    data = json.loads(raw)

    # 최소 방어
    for v in ("A", "B"):
        data.setdefault("variants", {}).setdefault(v, {})
        data["variants"][v].setdefault("faq", [])

    data.setdefault("notes", [])
    return data
