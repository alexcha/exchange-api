"""
korea_mofa_travel_alarm_scraper.py

대한민국 외교부(MOFA)가 공공데이터포털(data.go.kr)을 통해 제공하는
"국가·지역별 여행경보" 공식 Open API를 호출하여 파싱한 뒤,
미국 국무부 데이터와 동일한 스키마로 저장하는 스크립트.

데이터 출처 (공식, 공공데이터포털 등록 Open API):
  https://www.data.go.kr/data/15000827/openapi.do   (여행경보제도)
  요청 주소: https://apis.data.go.kr/1262000/TravelAlarmService2/getTravelAlarmList2

전제 조건 (중요, 무료지만 사전 신청 필요):
  1. https://www.data.go.kr 에서 회원가입
  2. 위 API("외교부_국가·지역별 여행경보") 페이지에서 "활용신청"
  3. 승인 후 마이페이지에서 발급된 서비스키(인증키)를 아래
     환경변수 DATA_GO_KR_SERVICE_KEY 로 설정
       export DATA_GO_KR_SERVICE_KEY="발급받은_인증키"
     ⚠️ 반드시 "일반 인증키(Decoding)" 값을 사용할 것.
        "Encoding" 키를 넣으면 requests가 URL 인코딩을 한 번 더 해서
        키가 깨지고 403 Forbidden이 발생합니다.
  개발계정 기준 1일 트래픽 10,000건까지 무료.

주의 (중요):
  이 스크립트를 작성한 환경은 data.go.kr 상세페이지 접근이
  robots.txt로 차단되어 있어 요청 파라미터명/응답 필드명을
  100% 실제 호출로 확인하지 못했습니다. 검색 결과에 노출된
  공식 요약 정보(요청변수/출력결과 목록)를 근거로 아래와 같이
  작성했습니다:
    - 요청변수: 인증키(serviceKey), 페이지번호(pageNo),
      페이지당개수(numOfRows), 반환타입(type), 국가명, ISO코드
    - 출력결과: 국가영문명, 국가한글명, ISO 2자리코드, 대륙코드/명,
      경보단계, 지역유형, 경보내용, 작성일
  실제 실행 시 응답 JSON을 한 번 출력해서(print(raw)) 정확한 키
  이름을 확인 후 KEY_CANDIDATES를 조정해 주세요.

사용법:
    pip install requests --break-system-packages
    export DATA_GO_KR_SERVICE_KEY="발급받은_인증키"
    python korea_mofa_travel_alarm_scraper.py

출력:
    korea_mofa_travel_advisories.json (스크립트와 같은 디렉터리)
"""

import json
import os
import sys
import time
from datetime import datetime, timezone

import requests

BASE_URL = "https://apis.data.go.kr/1262000/TravelAlarmService2/getTravelAlarmList2"
USER_AGENT = "travel-advisory-research-script/1.0 (+contact: user)"
PAGE_SIZE = 100

# 한국 여행경보 4단계 -> 미국식 1~4 등급은 이미 동일한 체계이므로 별도 변환 불필요.
# 1단계: 여행유의 / 2단계: 여행자제 / 3단계: 출국권고 / 4단계: 여행금지


def get_service_key():
    key = os.environ.get("DATA_GO_KR_SERVICE_KEY")
    if not key:
        print(
            "환경변수 DATA_GO_KR_SERVICE_KEY 가 설정되어 있지 않습니다.\n"
            "https://www.data.go.kr 에서 '외교부_국가·지역별 여행경보' API를 "
            "신청한 뒤 발급받은 인증키를 설정해 주세요.",
            file=sys.stderr,
        )
        sys.exit(1)
    return key


def _first_present(d, keys, default=None):
    for k in keys:
        if isinstance(d, dict) and k in d and d[k] not in (None, ""):
            return d[k]
    return default


def fetch_page(service_key, page_no):
    params = {
        "serviceKey": service_key,
        "numOfRows": PAGE_SIZE,
        "pageNo": page_no,
        "type": "json",
    }
    resp = requests.get(
        BASE_URL, params=params, headers={"User-Agent": USER_AGENT}, timeout=30
    )
    resp.raise_for_status()
    return resp.json()


def extract_items(raw):
    """
    공공데이터포털 표준 응답 구조( response.body.items.item )를 우선 시도하고,
    실패 시 다른 흔한 변형 구조도 방어적으로 시도한다.
    """
    try:
        body = raw["response"]["body"]
        total_count = int(body.get("totalCount", 0))
        items = body.get("items", {})
        if isinstance(items, dict):
            items = items.get("item", [])
        if isinstance(items, dict):  # 결과가 1건이면 dict로만 오는 경우 방어
            items = [items]
        return items or [], total_count
    except (KeyError, TypeError):
        pass

    # 방어적 대체 경로
    items = raw.get("items") or raw.get("data") or []
    return items, len(items)


def parse_record(item):
    return {
        "iso_code": _first_present(item, ["isoCd", "iso_cd", "isoCode"]),
        "name": _first_present(item, ["countryEngNm", "country_eng_nm", "countryNm"]),
        "name_kr": _first_present(item, ["countryNm", "country_nm"]),
        "advisory_level": _to_int(
            _first_present(item, ["alarmLvl", "alarm_lvl", "level"])
        ),
        "advisory_text": _first_present(item, ["remark", "alarmCn", "content"]),
        "region_type": _first_present(item, ["areaType", "area_type"]),
        "continent": _first_present(item, ["continentEngNm", "continent_eng_nm"]),
        "last_updated": _first_present(item, ["wrtDt", "wrt_dt", "regDt"]),
        "risk_score": None,
    }


def _to_int(v):
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def main():
    service_key = get_service_key()

    print("[1/2] 외교부 여행경보 API 호출 중...", file=sys.stderr)
    all_items = []
    page_no = 1
    total_count = None
    while True:
        raw = fetch_page(service_key, page_no)
        items, total_count = extract_items(raw)
        if not items:
            break
        all_items.extend(items)
        print(f"      페이지 {page_no}: {len(items)}건 (누적 {len(all_items)}건)", file=sys.stderr)
        if total_count and len(all_items) >= total_count:
            break
        page_no += 1
        time.sleep(0.3)

    records = [parse_record(item) for item in all_items]

    output = {
        "status": "success",
        "source": "대한민국 외교부 (MOFA) - 해외안전여행 여행경보제도",
        "source_url": "https://www.0404.go.kr/",
        "api_url": BASE_URL,
        "license": "공공데이터포털 이용약관에 따름",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "count": len(records),
        "data": records,
    }

    out_path = "korea_mofa_travel_advisories.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"[2/2] 완료: {len(records)}건 저장 -> {out_path}", file=sys.stderr)


if __name__ == "__main__":
    main()
