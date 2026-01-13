from __future__ import annotations

from typing import Dict, List


BANNED_FAIL = [
    "완치", "치료", "염증 치료", "아토피 치료", "피부병 치료", "의약품", "부작용 없음", "100%"
]

BANNED_WARN = [
    "즉시", "무조건", "완벽", "기적", "단번에", "확실한 효과"
]


def qc_check_text_pack(text: str) -> Dict[str, object]:
    t = text or ""
    hits_fail = [w for w in BANNED_FAIL if w in t]
    hits_warn = [w for w in BANNED_WARN if w in t]

    if hits_fail:
        return {"grade": "FAIL", "hits": hits_fail, "notes": ["치료/의약품 오인/과장 표현 제거 필요"]}
    if hits_warn:
        return {"grade": "WARN", "hits": hits_warn, "notes": ["단정/과장 표현을 완화하면 더 안전함"]}
    return {"grade": "PASS", "hits": [], "notes": []}
