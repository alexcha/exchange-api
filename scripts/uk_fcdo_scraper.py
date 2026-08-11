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

이번 버전에서 바뀐 점 (병렬 처리):
  - 국가별 상세 조회(fetch_country_detail)를 순차 for 루프 + time.sleep(1초)
    대신 concurrent.futures.ThreadPoolExecutor로 동시 처리하도록 변경.
  - 226개국을 1req/sec로 순차 처리하면 약 4분 정도 걸리던 것을,
    MAX_WORKERS개 스레드로 나눠 처리해 크게 단축.
  - 서버에 대한 예의(politeness)는 유지하기 위해 스레드마다 요청 간
    소폭의 딜레이(REQUEST_DELAY_SEC)를 그대로 두고, 동시 연결 수는
    MAX_WORKERS로 제한함 (요청 폭주 방지).
  - 진행 상황 출력과 결과 리스트 접근은 스레드 안전을 위해 Lock으로 보호.
  - 결과 순서는 country_list의 원래 순서를 그대로 유지하도록 인덱스 기반으로 정렬.

사용법:
    pip install requests --break-system-packages
    python uk_fcdo_scraper.py

출력:
    uk_fcdo_advisories.json  (스크립트와 같은 디렉터리)
"""

import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from threading import Lock

import requests

CONTENT_API = "https://www.gov.uk/api/content"
INDEX_PATH = "/foreign-travel-advice"
USER_AGENT = "travel-advisory-research-script/1.0 (+contact: user)"
REQUEST_DELAY_SEC = 1.0  # 정중한 크롤링을 위한 스레드별 딜레이 (공식 문서 권장 사항 참고)
MAX_WORKERS = 8  # 동시 요청 수 (서버 부담을 고려해 과도하게 높이지 않음)

# alert_status 코드를 US 국무부 스타일의 1~4 등급으로 정규화하기 위한 매핑.
# 여러 alert_status가 동시에 붙는 경우, 가장 심각한 코드를 기준으로 판정한다.
ALERT_STATUS_SEVERITY = {
    # ⚠️ [버그 수정] 실제 GOV.UK Content API는 "_to_whole_country"/"_to_parts" 접미사가
    # 항상 붙어서 내려온다(접미사 없는 "avoid_all_travel"/"avoid_all_but_essential_travel"
    # 값은 실제 응답에 한 번도 안 나온다). 이전 버전은 접미사 없는 키만 등록해놔서
    # 실제 값과 전혀 매칭이 안 됐고, normalize_level()의 .get(status, 1) 기본값 1로
    # 항상 떨어졌다 - 그 결과 러시아/북한/이란/아프가니스탄/시리아/예멘/벨라루스처럼
    # "전국 여행 자제"(가장 심각한 등급)여야 할 나라들이 전부 1단계(정상)로 잘못
    # 저장되고 있었다. 접미사 있는 키를 추가하고, 혹시 모를 접미사 없는 값도
    # 대비 차원에서 함께 남겨둔다.
    "avoid_all_travel_to_whole_country": 4,
    "avoid_all_travel": 4,
    "avoid_all_but_essential_travel_to_whole_country": 3,
    "avoid_all_but_essential_travel": 3,
    "avoid_all_travel_to_parts": 3,          # 일부 지역 전체 여행 자제
    "avoid_all_but_essential_travel_to_parts": 2,
    "see_the_summary": 2,
    "notify_before_travel": 1,
}

# 진행 상황 카운터/출력 보호용 락
_progress_lock = Lock()
_completed_count = 0


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
    # 스레드별로 요청 사이에 소폭의 딜레이를 둬서 서버에 대한 예의를 유지한다.
    time.sleep(REQUEST_DELAY_SEC)

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


def _fetch_with_index(index, base_path, total):
    """ThreadPoolExecutor용 래퍼: (원래 순서 인덱스, 결과/예외) 반환."""
    global _completed_count
    try:
        record = fetch_country_detail(base_path)
        error = None
    except requests.RequestException as e:
        record = None
        error = e

    with _progress_lock:
        _completed_count += 1
        if error:
            print(f"      경고: {base_path} 가져오기 실패 - {error}", file=sys.stderr)
        if _completed_count % 20 == 0 or _completed_count == total:
            print(f"      진행: {_completed_count}/{total}", file=sys.stderr)

    return index, record


def main():
    print("[1/3] FCDO 여행경보 국가 목록 가져오는 중... (Content API 인덱스)", file=sys.stderr)
    country_list = fetch_country_list()
    total = len(country_list)
    print(f"      -> {total}개 국가/지역 발견", file=sys.stderr)

    print(
        f"[2/3] 국가별 상세 정보(alert_status) 병렬 파싱 중... "
        f"(동시 {MAX_WORKERS}개, 요청당 {REQUEST_DELAY_SEC}초 딜레이)",
        file=sys.stderr,
    )

    results = [None] * total
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [
            executor.submit(_fetch_with_index, i, item["base_path"], total)
            for i, item in enumerate(country_list)
            if item.get("base_path")
        ]
        for future in as_completed(futures):
            index, record = future.result()
            if record is not None:
                results[index] = record

    # 실패한(None) 항목은 제외하고, 원래 국가 목록 순서를 유지한다.
    records = [r for r in results if r is not None]

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
