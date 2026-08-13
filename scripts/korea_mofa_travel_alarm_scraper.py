"""
korea_mofa_special_travel_warning_scraper.py

대한민국 외교부(MOFA)가 공공데이터포털(data.go.kr)을 통해 제공하는
"국가∙지역별 특별여행주의보" 공식 Open API(SptravelWarningServiceV2)를
호출하여 파싱한 뒤 JSON으로 저장하는 스크립트.

데이터셋: 외교부_국가∙지역별 특별여행주의보 (data.go.kr ID: 15076244)
  https://www.data.go.kr/data/15076244/openapi.do

본 스크립트는 사용자가 제공한 공식 기술문서
  "외교부_국가∙지역별 특별여행주의보 Open API 활용가이드"
의 요청/응답 메시지 명세를 그대로 따른다 (추정이 아닌 확인된 스펙).

⚠️ 왜 이 스크립트가 필요한가 (배경):
  기존 korea_mofa_travel_alarm_scraper.py가 쓰는 TravelAlarmService2는
  "여행경보 1~4단계"만 다룬다. 그런데 외교부는 이것과 완전히 별개로
  "특별여행주의보"라는 독립 카테고리를 운영한다 - 단기적 위험 상황(전쟁,
  감염병 확산 등)이 생겼을 때 발령되는 경보로, 1~4단계 체계와는 다른
  트랙이다. 실제로 러시아는 TravelAlarmService2 상으로는 "우크라이나
  접경지역만 4단계"이고 국가 전체 등급이 아예 없는데, 나머지 전역은 이
  특별여행주의보(철수권고)가 걸려있다(0404.go.kr 공지사항으로 확인 완료).
  TravelAlarmService2만 긁어서는 이 정보를 알 수 없어서 별도로 긁는다.

⚠️ 응답 구조가 TravelAlarmService2와 다르다는 점에 유의:
  이 API는 "알람 레벨(alarm_lvl)" 같은 단일 숫자 등급이 없다. 대신 국가당
  최대 2가지의 독립된 경보를 각각 별도 필드 쌍으로 내려준다:
    - evacuate_region_ty / evacuate_rcmnd_remark   : "철수권고" (더 약한 단계)
    - forbidden_region_ty / forbidden_rcmnd_remark : "여행금지" (더 강한 단계)
  두 필드 다 지역 범위 값으로 "전체" 또는 "일부"가 들어가고, 둘 다 해당사항이
  없으면 null이다(예: 문서 예제의 가나=Ghana는 둘 다 null - 현재 특별여행
  주의보 대상이 아니라는 뜻). 최대 심각도를 정할 때는 forbidden(여행금지)이
  evacuate(철수권고)보다 더 심각한 단계로 취급한다.

전제 조건:
  1. https://www.data.go.kr 에서 "외교부_국가∙지역별 특별여행주의보"
     (데이터셋 ID 15076244) API를 검색해 별도로 활용신청
  2. 승인 후 발급된 서비스키를 환경변수로 설정
     export DATA_GO_KR_SPECIAL_WARNING_SERVICE_KEY="발급받은_인증키"
     ⚠️ [신규] 이 API는 TravelAlarmService2와 별개로 전용 인증키를 새로
     발급받았다. 기존 스크립트가 쓰는 DATA_GO_KR_SERVICE_KEY와 섞이지 않게
     환경변수 이름을 다르게 분리했다 - GitHub Actions Secrets에도 이
     이름 그대로 새 값을 등록해야 한다.

요청 파라미터 (기술문서 기준, cond[]는 옵션 - 생략하면 전체 목록을 페이지네이션):
  ServiceKey                   인증키 (필수)
  numOfRows                    한 페이지 결과 수 (필수)
  pageNo                       페이지 번호 (필수)
  returnType                   XML 또는 JSON (필수)
  cond[country_nm::EQ]         한글 국가명 (옵션 - 특정 국가만 조회할 때)
  cond[country_iso_alp2::EQ]   ISO 2자리 코드 (옵션)

사용법:
    pip install requests --break-system-packages
    export DATA_GO_KR_SPECIAL_WARNING_SERVICE_KEY="발급받은_인증키"
    python korea_mofa_special_travel_warning_scraper.py

출력:
    korea_mofa_special_travel_warnings.json
    (evacuate_region_ty/forbidden_region_ty 둘 다 null인, 즉 현재 특별여행
     주의보 대상이 아닌 국가는 결과에서 제외하고 저장한다)
"""

import json
import os
import sys
import time
from datetime import datetime, timezone
from urllib.parse import unquote

import requests

# ⚠️ [정정] 기술문서 표에는 http("전송 레벨 암호화: 없음")로 적혀있었지만,
# 실제 data.go.kr 활용신청 페이지의 요청주소는 https였다(사용자 확인).
# 공공데이터포털 API들이 http에서 https로 전환되는 경우가 흔한데 기술문서가
# 그 변경을 반영 못 한 것으로 보인다. 실제 요청주소를 그대로 신뢰한다.
BASE_URL = "https://apis.data.go.kr/1262000/SptravelWarningServiceV2/getSpTravelWarningListV2"
USER_AGENT = "travel-advisory-research-script/1.0 (+contact: user)"
PAGE_SIZE = 100

# 재시도 설정: 국내 인프라 API 특성상 해외 리전(GitHub Actions 러너)에서
# 간헐적으로 커넥션이 막히는 문제를 완화하기 위함 (형제 API에서 겪었던 것과 동일).
MAX_RETRIES = 5
RETRY_BACKOFF_BASE_SEC = 5
CONNECT_TIMEOUT_SEC = 20
READ_TIMEOUT_SEC = 30

# 기술문서 2장 "OpenAPI 에러 코드정리" 기준 (이 API 고유의 에러 코드 체계 -
# TravelAlarmService2와 코드 값 자체가 다르므로 별도로 정의)
RESULT_CODE_MESSAGES = {
    "1": "APPLICATION_ERROR - 어플리케이션 에러",
    "10": "INVALID_REQUEST_PARAMETER_ERROR - 잘못된 요청 파라메터 에러",
    "12": "NO_OPENAPI_SERVICE_ERROR - 해당 오픈API서비스가 없거나 폐기됨",
    "20": "SERVICE_ACCESS_DENIED_ERROR - 서비스 접근거부",
    "22": "LIMITED_NUMBER_OF_SERVICE_REQUESTS_EXCEEDS_ERROR - 서비스 요청제한횟수 초과",
    "30": "SERVICE_KEY_IS_NOT_REGISTERED_ERROR - 등록되지 않은 서비스키",
    "31": "DEADLINE_HAS_EXPIRED_ERROR - 기한만료된 서비스키",
    "32": "UNREGISTERED_IP_ERROR - 등록되지 않은 IP",
    "99": "UNKNOWN_ERROR - 기타에러",
}


def get_service_key():
    key = os.environ.get("DATA_GO_KR_SPECIAL_WARNING_SERVICE_KEY")
    if not key:
        print(
            "환경변수 DATA_GO_KR_SPECIAL_WARNING_SERVICE_KEY 가 설정되어 있지 않습니다.\n"
            "https://www.data.go.kr 에서 '외교부_국가∙지역별 특별여행주의보'(15076244) "
            "API를 신청한 뒤 발급받은 전용 인증키를 설정해 주세요.",
            file=sys.stderr,
        )
        sys.exit(1)

    # korea_mofa_travel_alarm_scraper.py와 동일한 이유로 Encoding/Decoding
    # 키 둘 다 안전하게 처리 (더블 URL 인코딩 방지).
    decoded_key = unquote(key)
    if decoded_key != key:
        print(
            "[알림] 서비스키에 URL 인코딩된 문자가 감지되어 디코딩했습니다.",
            file=sys.stderr,
        )
    return decoded_key


def fetch_page(service_key, page_no):
    params = {
        "ServiceKey": service_key,  # ⚠️ 이 API는 문서상 대문자 S로 시작하는 "ServiceKey" (형제 API는 소문자 serviceKey)
        "returnType": "JSON",
        "numOfRows": PAGE_SIZE,
        "pageNo": page_no,
    }

    last_error = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(
                BASE_URL,
                params=params,
                headers={"User-Agent": USER_AGENT},
                timeout=(CONNECT_TIMEOUT_SEC, READ_TIMEOUT_SEC),
            )
        except (requests.exceptions.ConnectTimeout, requests.exceptions.ConnectionError) as e:
            last_error = e
            if attempt == MAX_RETRIES:
                break
            wait = RETRY_BACKOFF_BASE_SEC * (2 ** (attempt - 1))
            print(
                f"      [재시도 {attempt}/{MAX_RETRIES}] 페이지 {page_no} 커넥션 실패 "
                f"({type(e).__name__}) - {wait}초 후 재시도...",
                file=sys.stderr,
            )
            time.sleep(wait)
            continue

        if not resp.ok:
            print(
                f"[디버그] HTTP {resp.status_code} 응답 본문:\n{resp.text[:2000]}",
                file=sys.stderr,
            )
        resp.raise_for_status()
        raw = resp.json()

        header = raw.get("response", {}).get("header", {})
        result_code = str(header.get("resultCode", "0"))
        if result_code not in ("0", "00"):
            msg = header.get("resultMsg") or RESULT_CODE_MESSAGES.get(result_code, "알 수 없는 에러")
            print(f"[API 에러] resultCode={result_code}, resultMsg={msg}", file=sys.stderr)
            sys.exit(1)

        return raw

    print(
        f"[실패] 페이지 {page_no}: {MAX_RETRIES}회 재시도 후에도 연결 실패 - {last_error}",
        file=sys.stderr,
    )
    raise last_error


def parse_record(item):
    """
    기술문서에서 확인된 실제 필드명 그대로 매핑.
    evacuate(철수권고)/forbidden(여행금지) 두 축을 모두 보존해서 저장하고,
    앱 쪽(world_map.js)에서 "무엇이든 하나라도 있으면 특별여행주의보 대상"
    으로 판단할 수 있게 한다. forbidden이 evacuate보다 심각도가 높다.
    """
    return {
        "iso_code": item.get("country_iso_alp2"),
        "name": item.get("country_eng_nm"),
        "name_kr": item.get("country_nm"),
        "continent_code": item.get("continent_cd"),
        "continent": item.get("continent_eng_nm"),
        "continent_kr": item.get("continent_nm"),
        "evacuate_region_type": item.get("evacuate_region_ty") or None,      # "전체" | "일부" | None
        "evacuate_remark": item.get("evacuate_rcmnd_remark") or None,
        "forbidden_region_type": item.get("forbidden_region_ty") or None,    # "전체" | "일부" | None
        "forbidden_remark": item.get("forbidden_rcmnd_remark") or None,
        "danger_map_url": item.get("dang_map_download_url") or None,
        "flag_url": item.get("flag_download_url") or None,
        "map_url": item.get("map_download_url") or None,
        "last_updated": item.get("written_dt") or None,
        "risk_score": None,
    }


def has_active_warning(record):
    """evacuate/forbidden 둘 다 비어있으면(=None) 특별여행주의보 대상이 아닌 국가."""
    return bool(record["evacuate_region_type"]) or bool(record["forbidden_region_type"])


def main():
    service_key = get_service_key()

    print("[1/2] 외교부 특별여행주의보 API 호출 중...", file=sys.stderr)
    all_items = []
    page_no = 1
    total_count = None

    while True:
        raw = fetch_page(service_key, page_no)
        body = raw.get("response", {}).get("body", {})
        items = body.get("items", {})
        if isinstance(items, dict):
            items = items.get("item", [])
        if isinstance(items, dict):
            items = [items]
        total_count = body.get("totalCount", total_count)

        if not items:
            break
        all_items.extend(items)
        print(f"      페이지 {page_no}: {len(items)}건 (누적 {len(all_items)}건 / 전체 {total_count})", file=sys.stderr)
        if total_count and len(all_items) >= int(total_count):
            break
        page_no += 1
        time.sleep(0.3)

    all_records = [parse_record(item) for item in all_items]
    # ⚠️ 이 API는 모든 국가에 대해 레코드를 내려준다(문서 예제의 가나처럼
    # 특별여행주의보가 없는 나라도 evacuate/forbidden이 null인 채로 포함됨).
    # 앱에 쓸모없는 "아무 경보도 없음" 레코드는 걸러내고 저장한다.
    active_records = [r for r in all_records if has_active_warning(r)]

    output = {
        "status": "success",
        "source": "대한민국 외교부 (MOFA) - 특별여행주의보",
        "source_url": "https://www.0404.go.kr/",
        "api_url": BASE_URL,
        "license": "공공데이터포털 이용약관에 따름",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "count": len(active_records),
        "total_countries_checked": len(all_records),
        "data": active_records,
    }

    out_path = "korea_mofa_special_travel_warnings.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(
        f"[2/2] 완료: 전체 {len(all_records)}개국 중 특별여행주의보 대상 "
        f"{len(active_records)}건 저장 -> {out_path}",
        file=sys.stderr,
    )


if __name__ == "__main__":
    main()
