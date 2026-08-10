"""
canada_travel_advisory_scraper.py

캐나다 외교부(Global Affairs Canada, GAC)가 공식적으로 제공하는
Travel Advice and Advisories 오픈데이터를 가져와 파싱한 뒤,
미국 국무부(State Dept) 데이터와 동일한 스키마로 저장하는 스크립트.

데이터 출처 (공식, 무인증, 공개 JSON):
  https://data.international.gc.ca/travel-voyage/index-alpha-eng.json

참고:
  - Open Government Portal 등록:
    https://open.canada.ca/data/en/dataset/bef2ebb3-ca9a-485f-aaff-5dc36eb89426
  - 라이선스: Open Government Licence - Canada

GAC의 advisory-state 값은 0~3의 4단계이며 다음과 같이 매핑된다.
  0 -> Exercise normal security precautions      (평시 수준의 주의)
  1 -> Exercise a high degree of caution          (고도의 주의)
  2 -> Avoid non-essential travel                 (필수적이지 않은 여행 자제)
  3 -> Avoid all travel                           (모든 여행 자제)

US 국무부 advisory_level(1~4) 체계와 맞추기 위해 advisory-state + 1 로
정규화한다.

사용법:
    pip install requests --break-system-packages
    python canada_travel_advisory_scraper.py

출력:
    canada_travel_advisories.json (스크립트와 같은 디렉터리)
"""

import json
import sys
from datetime import datetime, timezone

import requests

SOURCE_URL = "https://data.international.gc.ca/travel-voyage/index-alpha-eng.json"
USER_AGENT = "travel-advisory-research-script/1.0 (+contact: user)"


def fetch_raw_data():
    resp = requests.get(SOURCE_URL, headers={"User-Agent": USER_AGENT}, timeout=30)
    resp.raise_for_status()
    return resp.json()


def is_sovereign_country_code(iso_code):
    """
    캐나다 데이터셋에는 아조레스(PT-20), 카나리아 제도(IC) 등
    ISO 3166-1 alpha-2가 아닌 하위 지역 코드도 섞여 있다.
    2자리 알파벳 코드만 국가로 간주(단순 필터링, 완벽하지 않을 수 있음).
    """
    return len(iso_code) == 2 and iso_code.isalpha()


def parse_records(raw):
    data = raw.get("data", {})
    records = []
    for iso_code, entry in data.items():
        if not is_sovereign_country_code(iso_code):
            continue

        advisory_state = entry.get("advisory-state")
        if advisory_state is None:
            continue

        eng = entry.get("eng", {})
        date_published = (entry.get("date-published") or {}).get("date")

        records.append(
            {
                "iso_code": iso_code,
                "name": entry.get("country-eng"),
                "advisory_level": int(advisory_state) + 1,  # 0~3 -> 1~4
                "risk_score": None,
                "advisory_text": eng.get("advisory-text"),
                "has_regional_advisory": bool(entry.get("has-regional-advisory")),
                "last_updated": date_published,
                "web_url": f"https://travel.gc.ca/destinations/{eng.get('url-slug', '')}",
            }
        )
    return records


def main():
    print("[1/2] Global Affairs Canada 공식 JSON 피드 가져오는 중...", file=sys.stderr)
    raw = fetch_raw_data()
    generated_meta = raw.get("metadata", {}).get("generated", {})
    print(f"      -> 원본 데이터 기준시각: {generated_meta.get('date')}", file=sys.stderr)

    records = parse_records(raw)
    print(f"      -> {len(records)}개국 파싱 완료", file=sys.stderr)

    output = {
        "status": "success",
        "source": "Global Affairs Canada (GAC) Travel Advice and Advisories",
        "source_url": "https://travel.gc.ca/travelling/advisories",
        "license": "Open Government Licence - Canada",
        "source_generated_at": generated_meta.get("date"),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "count": len(records),
        "data": records,
    }

    out_path = "canada_travel_advisories.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"[2/2] 완료: {len(records)}건 저장 -> {out_path}", file=sys.stderr)


if __name__ == "__main__":
    main()
