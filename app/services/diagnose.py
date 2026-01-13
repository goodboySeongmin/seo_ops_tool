# app/services/diagnose.py
from typing import Any, Dict
import re
from .qc_rules import run_qc

def _len_ok(n: int, lo: int, hi: int) -> bool:
    return lo <= n <= hi

def diagnose(page_text: str, meta_title: str | None, meta_desc: str | None) -> Dict[str, Any]:
    # 간단 CRO 휴리스틱 (MVP)
    cta_hits = len(re.findall(r"(구매|신청|상담|지금|바로)\w*", page_text))
    has_faq = "FAQ" in page_text or "자주" in page_text

    # SEO 길이 체크 (권장 범위는 상황마다 다르지만 MVP 기준으로만)
    title_len = len(meta_title or "")
    desc_len = len(meta_desc or "")

    seo = {
        "meta_title_len": title_len,
        "meta_desc_len": desc_len,
        "meta_title_ok": _len_ok(title_len, 20, 60) if meta_title else False,
        "meta_desc_ok": _len_ok(desc_len, 60, 170) if meta_desc else False,
        "has_faq": has_faq,
    }

    qc = run_qc((meta_title or "") + "\n" + (meta_desc or "") + "\n" + page_text)

    cro = {
        "cta_hits": cta_hits,
        "cta_ok": cta_hits >= 2,
        "above_fold_offer_hint": ("할인" in page_text) or ("무료배송" in page_text),
    }

    # 점수는 “전후 비교용”으로만 단순화
    score = 0
    score += 25 if seo["meta_title_ok"] else 0
    score += 25 if seo["meta_desc_ok"] else 0
    score += 20 if cro["cta_ok"] else 0
    score += 10 if cro["above_fold_offer_hint"] else 0
    score += 20 if qc.grade == "PASS" else (10 if qc.grade == "WARN" else 0)

    issues = []
    if not seo["meta_title_ok"]:
        issues.append("Meta title 길이/구성이 비정상(권장 범위 이탈).")
    if not seo["meta_desc_ok"]:
        issues.append("Meta description 길이/구성이 비정상(권장 범위 이탈).")
    if not cro["cta_ok"]:
        issues.append("CTA 노출이 약함(상단/중단/하단 반복 부족).")
    if qc.grade != "PASS":
        issues.append(f"컴플라이언스 리스크: {qc.grade} / hits={qc.hits}")

    return {
        "score": score,
        "seo": seo,
        "cro": cro,
        "qc": {"grade": qc.grade, "hits": qc.hits, "notes": qc.notes},
        "issues": issues
    }
