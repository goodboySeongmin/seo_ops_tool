from __future__ import annotations

import math
from typing import Any, Dict, List


def _phi_cdf(x: float) -> float:
    # 표준정규 CDF
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def _p_value_two_sided(z: float) -> float:
    return 2.0 * (1.0 - _phi_cdf(abs(z)))


def ab_ctr_summary(events: List[Dict[str, Any]], min_views_required: int = 20) -> Dict[str, Any]:
    A_view = sum(1 for e in events if e.get("variant") == "A" and e.get("event_name") == "view")
    A_click = sum(1 for e in events if e.get("variant") == "A" and e.get("event_name") == "cta_click")
    B_view = sum(1 for e in events if e.get("variant") == "B" and e.get("event_name") == "view")
    B_click = sum(1 for e in events if e.get("variant") == "B" and e.get("event_name") == "cta_click")

    A_ctr = (A_click / A_view) if A_view > 0 else 0.0
    B_ctr = (B_click / B_view) if B_view > 0 else 0.0
    uplift = (B_ctr - A_ctr)

    # z-test for two proportions (pooled)
    z = 0.0
    pval = 1.0
    if A_view > 0 and B_view > 0:
        p_pool = (A_click + B_click) / (A_view + B_view) if (A_view + B_view) > 0 else 0.0
        denom = math.sqrt(max(p_pool * (1 - p_pool) * (1 / A_view + 1 / B_view), 1e-12))
        z = (B_ctr - A_ctr) / denom
        pval = _p_value_two_sided(z)

    recommend = None
    # 최소 샘플 기준 충족 + p<0.1이면 추천
    if A_view >= min_views_required and B_view >= min_views_required and pval < 0.1:
        recommend = "B" if B_ctr > A_ctr else "A"

    return {
        "A": {"view": A_view, "click": A_click, "ctr": A_ctr},
        "B": {"view": B_view, "click": B_click, "ctr": B_ctr},
        "uplift": uplift,
        "z": z,
        "p_value": pval,
        "min_views_required": min_views_required,
        "recommend_variant": recommend
    }
