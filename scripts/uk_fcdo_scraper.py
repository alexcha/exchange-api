"""
uk_fcdo_scraper.py

영국 외교부(FCDO, Foreign, Commonwealth & Development Office)의
공식 Foreign Travel Advice 데이터를 GOV.UK Content API에서
크롤링/파싱하여 JSON으로 저장하는 스크립트.

데이터 출처 (공식, 무인증, 공개):
  - 국가 목록: https://www.gov.uk/api/content/foreign-travel-advice
    (이 인덱스 문서의 links.children 배열에 전체 국가의 base_path가 들어있음)
  - 국가별 상세: https://www.gov.uk/api/content/foreign-travel-advice/<slug>

⚠️ 이전 버전은 검색 API(/api/search.json?filter_format=travel_advice)를
   사용했는데, GOV.UK 검색 API의 filter_format 파라미터 검증 규칙이
   바뀌면서 HTTP 422 (Unknown Error)가 발생했다. 검색 API 없이도
   Content API 인덱스 문서 하나로 전체 국가 목록을 직접 얻을 수 있어
   더 간단하고 안정적이므로 이 방식으로 교체함.

참고:
  - GOV.UK Content API 공식 문서: https://content-api.publishing.service.gov.uk/
  - 라이선스: Open Government Licence v3.0 (Crown copyright)

FCDO는 미국 국무부처럼 "Level 1~4" 숫자 등급을 쓰지 않고
alert_status 코드(예: avoid_all_travel, avoid_all_travel_to_parts 등)로
표기합니다. 이 스크립트는 비교 편의를 위해 자체적으로 1~4 등급으로
정규화한 advisory_level 필드를 함께 만들어 US 국무부 데이터와
동일한 스키마로 저장합니다.

사용법:
    pip install requests --break-system-packages
    python uk_fcdo_scraper.py

출력:
    uk_fcdo_advisories.json  (스크립트와 같은 디렉터리)
"""

import json
import time
import sys
from datetime import datetime, timezone

import requests

CONTENT_API = "https://www.gov.uk/api/content"
INDEX_PATH = "/foreign-travel-advice"
USER_AGENT = "travel-advisory-research-script/1.0 (+contact: user)"
REQUEST_DELAY_SEC = 1.0  # 정중한 크롤링을 위한 딜레이 (공식 문서 권장 사항 참고)

# alert_status 코드를 US 국무부 스타일의 1~4 등급으로 정규화하기 위한 매핑.
# 여러 alert_status가 동시에 붙는 경우, 가장 심각한 코드를 기준으로 판정한다.
ALERT_STATUS_SEVERITY = {
    "avoid_all_travel": 4,
    "avoid_all_travel_to_parts": 3,          # 일부 지역 전체 여행 자제
    "avoid_all_but_essential_travel": 3,
    "avoid_all_but_essential_travel_to_parts": 2,
    "see_the_summary": 2,
    "notify_before_travel": 1,
}


def fetch_country_list():
    """
    Content API 인덱스 문서(/api/content/foreign-travel-advice)를 가져와
    links.children 배열에서 전체 국가의 base_path를 추출한다.
    검색 API를 전혀 거치지 않으므로 filter_format 관련 422 에러가 나지 않는다.
    """
    url = f"{CONTENT_API}{INDEX_PATH}"
    resp = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=30)
    resp.raise_for_status()
    index_doc = resp.json()

    children = index_doc.get("links", {}).get("children", [])
    countries = []
    for child in children:
        base_path = child.get("base_path")
        if not base_path:
            continue
        countries.append(
            {
                "base_path": base_path,
                "title": child.get("title"),
                "public_updated_at": child.get("public_updated_at"),
            }
        )
    return countries


def normalize_level(alert_status_list):
    """alert_status 문자열 리스트를 1~4 정수 등급으로 정규화."""
    if not alert_status_list:
        return 1
    best = 1
    for status in alert_status_list:
        level = ALERT_STATUS_SEVERITY.get(status, 1)
        if level > best:
            best = level
    return best


def fetch_country_detail(base_path):
    """국가 하나의 상세 페이지(Content API)를 가져와 필요한 필드만 추출."""
    url = f"{CONTENT_API}{base_path}"
    resp = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=30)
    if resp.status_code == 303:
        # 일부 하위 경로는 상위 문서로 리다이렉트됨 (예: /local-laws-and-customs)
        redirect_url = resp.headers.get("Location")
        if redirect_url:
            resp = requests.get(redirect_url, headers={"User-Agent": USER_AGENT}, timeout=30)
    resp.raise_for_status()
    content = resp.json()

    details = content.get("details", {}) or {}
    alert_status = details.get("alert_status", []) or []

    return {
        "name": content.get("title", "").replace(" travel advice", "").strip(),
        "slug": base_path.strip("/").split("/")[-1],
        "alert_status_raw": alert_status,
        "advisory_level": normalize_level(alert_status),
        "risk_score": None,
        "summary": content.get("description", ""),
        "public_updated_at": content.get("public_updated_at"),
        "web_url": f"https://www.gov.uk{base_path}",
    }


def main():
    print("[1/3] FCDO 여행경보 국가 목록 가져오는 중... (Content API 인덱스)", file=sys.stderr)
    country_list = fetch_country_list()
    print(f"      -> {len(country_list)}개 국가/지역 발견", file=sys.stderr)

    print("[2/3] 국가별 상세 정보(alert_status) 파싱 중... (1 req/sec)", file=sys.stderr)
    records = []
    for i, item in enumerate(country_list, start=1):
        base_path = item.get("base_path")
        if not base_path:
            continue
        try:
            record = fetch_country_detail(base_path)
            records.append(record)
        except requests.RequestException as e:
            print(f"      경고: {base_path} 가져오기 실패 - {e}", file=sys.stderr)
        if i % 20 == 0:
            print(f"      진행: {i}/{len(country_list)}", file=sys.stderr)
        time.sleep(REQUEST_DELAY_SEC)

    output = {
        "status": "success",
        "source": "UK FCDO (gov.uk Content API)",
        "source_url": "https://www.gov.uk/foreign-travel-advice",
        "license": "Open Government Licence v3.0 (Crown copyright)",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "count": len(records),
        "data": records,
    }

    out_path = "uk_fcdo_advisories.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"[3/3] 완료: {len(records)}건 저장 -> {out_path}", file=sys.stderr)


if __name__ == "__main__":
    main()
