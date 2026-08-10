"""
australia_smartraveller_scraper.py

호주 외교통상부(DFAT)가 운영하는 Smartraveller의 공식 공개 API에서
여행경보 데이터를 가져와 파싱한 뒤, 미국 국무부 데이터와 동일한
스키마로 저장하는 스크립트.

데이터 출처 (공식, 무인증, 공개):
  https://www.smartraveller.gov.au/destinations-export

참고:
  - 공식 안내 페이지("we provide a free public API"):
    https://www.smartraveller.gov.au/consular-services/resources
  - Smartraveller는 1~4단계 advice level 체계를 사용한다.
      Level 1: Exercise normal safety precautions (평시 수준의 주의)
      Level 2: Exercise a high degree of caution   (고도의 주의)
      Level 3: Reconsider your need to travel       (여행 필요성 재고)
      Level 4: Do not travel                        (여행 금지)

주의 (중요):
  이 스크립트를 만든 환경에서는 자동화 크롤링 도구에 대해
  smartraveller.gov.au의 robots.txt가 접근을 막고 있어 실제 응답
  스키마를 직접 확인하지 못했습니다. 아래 파서는 공식적으로 알려진
  필드명(및 흔히 쓰이는 변형)을 방어적으로 모두 처리하도록 작성했지만,
  실제 실행 시 아래 `RAW_FIELD_CANDIDATES`를 응답 구조에 맞게
  한 번 확인/조정해 주세요. (print(raw[0]) 등으로 실제 키를 확인 후 조정)

사용법:
    pip install requests --break-system-packages
    python australia_smartraveller_scraper.py

출력:
    australia_smartraveller_advisories.json (스크립트와 같은 디렉터리)
"""

import json
import sys
from datetime import datetime, timezone

import requests

SOURCE_URL = "https://www.smartraveller.gov.au/destinations-export"
USER_AGENT = "travel-advisory-research-script/1.0 (+contact: user)"


def fetch_raw_data():
    resp = requests.get(SOURCE_URL, headers={"User-Agent": USER_AGENT}, timeout=30)
    resp.raise_for_status()
    return resp.json()


def _first_present(d, keys, default=None):
    """딕셔너리 d에서 keys 중 처음으로 존재하는 값을 반환 (필드명 변형 방어)."""
    for k in keys:
        if isinstance(d, dict) and k in d and d[k] not in (None, ""):
            return d[k]
    return default


def parse_records(raw):
    """
    Smartraveller export의 최상위 구조는 국가 리스트(list) 또는
    {"destinations": [...]} 형태의 dict일 수 있으므로 둘 다 처리한다.
    """
    if isinstance(raw, dict):
        items = raw.get("destinations") or raw.get("data") or raw.get("results") or []
    elif isinstance(raw, list):
        items = raw
    else:
        items = []

    records = []
    for item in items:
        country_obj = item.get("country") if isinstance(item.get("country"), dict) else {}

        name = _first_present(item, ["name", "title"]) or _first_present(
            country_obj, ["name"]
        )
        iso_code = _first_present(
            item, ["iso", "iso2", "alpha2", "isoCode", "countryCode"]
        ) or _first_present(country_obj, ["alpha2", "iso2"])
        level = _first_present(item, ["level", "adviceLevel", "advice_level"])
        advice_text = _first_present(
            item, ["advice", "adviceHeadline", "summary", "latestUpdate"]
        )
        published = _first_present(
            item, ["published", "publishedDate", "lastUpdated", "updated"]
        )
        page_url = _first_present(item, ["pageUrl", "url", "link"])

        if not name:
            continue

        records.append(
            {
                "iso_code": (iso_code or "").upper(),
                "name": name,
                "advisory_level": int(level) if level is not None else None,
                "risk_score": None,
                "advisory_text": advice_text,
                "last_updated": published,
                "web_url": page_url,
            }
        )
    return records


def main():
    print("[1/2] Smartraveller(DFAT) 공식 export 가져오는 중...", file=sys.stderr)
    try:
        raw = fetch_raw_data()
    except requests.RequestException as e:
        print(f"요청 실패: {e}", file=sys.stderr)
        print(
            "robots.txt 또는 User-Agent 차단일 수 있습니다. "
            "브라우저에서 URL을 열어 응답을 저장한 뒤 로컬 파일을 읽도록 "
            "스크립트를 수정해도 됩니다.",
            file=sys.stderr,
        )
        sys.exit(1)

    records = parse_records(raw)
    print(f"      -> {len(records)}개 목적지 파싱 완료", file=sys.stderr)
    if records:
        print(
            "      (필드가 비어 있다면 스크립트 상단 주석대로 "
            "RAW_FIELD_CANDIDATES를 실제 응답에 맞춰 조정하세요)",
            file=sys.stderr,
        )

    output = {
        "status": "success",
        "source": "Smartraveller (Australian DFAT)",
        "source_url": "https://www.smartraveller.gov.au/",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "count": len(records),
        "data": records,
    }

    out_path = "australia_smartraveller_advisories.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"[2/2] 완료: {len(records)}건 저장 -> {out_path}", file=sys.stderr)


if __name__ == "__main__":
    main()
