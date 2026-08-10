"""
korea_mofa_travel_alarm_scraper.py

대한민국 외교부(MOFA)가 공공데이터포털(data.go.kr)을 통해 제공하는
"국가·지역별 여행경보" 공식 Open API(TravelAlarmService2)를 호출하여
파싱한 뒤, 미국 국무부 데이터와 유사한 스키마로 저장하는 스크립트.

본 스크립트는 공식 기술문서
  "국가∙지역별 여행경보 - Open API 활용가이드 v1.4"
의 요청/응답 메시지 명세를 그대로 따른다.

데이터 출처 (공식, 공공데이터포털 등록 Open API):
  https://www.data.go.kr/data/15076237/openapi.do
  서비스 URL(문서 명시, SSL 미지원 - 반드시 http 사용):
    http://apis.data.go.kr/1262000/TravelAlarmService2
  요청 주소:
    http://apis.data.go.kr/1262000/TravelAlarmService2/getTravelAlarmList2

전제 조건 (무료, 사전 활용신청 필요 - 개발단계 자동승인):
  1. https://www.data.go.kr 에서 회원가입
  2. "외교부_국가∙지역별 여행경보" API 페이지에서 "활용신청"
  3. 승인 후 마이페이지에서 발급된 서비스키(인증키)를 아래
     환경변수 DATA_GO_KR_SERVICE_KEY 로 설정
       export DATA_GO_KR_SERVICE_KEY="발급받은_인증키"
     ⚠️ 반드시 "일반 인증키(Decoding)" 값을 사용할 것.
        "Encoding" 키를 넣으면 requests가 URL 인코딩을 한 번 더 해서
        키가 깨집니다.

요청 파라미터 (기술문서 기준):
  serviceKey            인증키 (필수)
  returnType             XML 또는 JSON (필수) - "type" 아님, "returnType"
  numOfRows             한 페이지 결과 수 (필수)
  pageNo                페이지 번호 (필수)
  cond[country_nm::EQ]        한글 국가명 (옵션)
  cond[country_iso_alp2::EQ]  ISO 2자리코드 (옵션)

응답 구조 (기술문서 기준, 중첩 없이 최상위에 바로 data 배열):
  {
    "resultCode": 0, "resultMsg": "정상",
    "numOfRows": 10, "pageNo": 1,
    "totalCount": 1268, "currentCount": 10,
    "data": [
      {
        "country_eng_nm": "...", "country_nm": "...",
        "country_iso_alp2": "...", "continent_cd": "...",
        "continent_eng_nm": "...", "continent_nm": "...",
        "dang_map_download_url": "...", "flag_download_url": "...",
        "map_download_url": "...", "alarm_lvl": "...",
        "remark": "...", "region_ty": "...", "written_dt": "..."
      }, ...
    ]
  }

resultCode 에러 코드 (기술문서 기준):
  0: 정상 | -1: 시스템 내부 오류 | -2: 잘못된 파라미터
  -3: 등록되지 않은 서비스 | -4: 등록되지 않은 인증키
  -9: 종료된 서비스 | -10: 트래픽 초과 | -401: 유효하지 않은 인증키
  -999: UNKNOWN

⚠️ 안정성 관련 (이번 버전에서 추가됨):
  apis.data.go.kr은 gov.uk 등 CDN 뒤에 있는 해외 API와 달리 국내
  인프라에 직접 호스팅되어 있어, GitHub Actions 호스티드 러너처럼
  해외 리전 IP에서 접속하면 간헐적으로 TCP 커넥션이 열리지 않고
  30초 커넥션 타임아웃이 나는 경우가 있다 (ConnectTimeoutError).
  이를 완화하기 위해 fetch_page()에 지수 백오프(exponential backoff)
  재시도 로직을 추가했다. 커넥션 타임아웃/일시적 네트워크 오류에는
  재시도하고, resultCode 기반의 API 레벨 에러(인증키 오류 등)는
  재시도해도 소용없으므로 즉시 종료한다.

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

# ⚠️ 기술문서에 "전송 레벨 암호화: 없음"으로 명시됨 -> https 아님, http 사용.
BASE_URL = "http://apis.data.go.kr/1262000/TravelAlarmService2/getTravelAlarmList2"
USER_AGENT = "travel-advisory-research-script/1.0 (+contact: user)"
PAGE_SIZE = 100

# 재시도 설정: 국내 인프라 API 특성상 해외 리전(Actions 러너)에서
# 간헐적으로 커넥션이 막히는 문제를 완화하기 위함.
MAX_RETRIES = 5
RETRY_BACKOFF_BASE_SEC = 5  # 1차 재시도 전 5초, 이후 지수적으로 증가
CONNECT_TIMEOUT_SEC = 20
READ_TIMEOUT_SEC = 30

RESULT_CODE_MESSAGES = {
    0: "정상",
    -1: "시스템 내부 오류가 발생하였습니다.",
    -2: "요청하신 파라미터가 적합하지 않습니다.",
    -3: "등록되지 않은 서비스입니다.",
    -4: "등록되지 않은 인증키입니다.",
    -9: "종료된 서비스입니다.",
    -10: "트래픽 허용 횟수를 초과하였습니다.",
    -401: "유효하지 않은 인증키입니다.",
    -999: "UNKNOWN",
}


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


def fetch_page(service_key, page_no):
    """
    페이지 하나를 가져온다. 커넥션 타임아웃/네트워크 오류가 나면
    지수 백오프로 최대 MAX_RETRIES회까지 재시도한다.
    API가 정상 응답했지만 resultCode != 0인 경우(인증키 오류 등)는
    재시도해도 의미가 없으므로 즉시 종료한다.
    """
    params = {
        "serviceKey": service_key,
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

        # 실제 응답은 표준 공공데이터포털 포맷: response.header / response.body
        # (기술문서 v1.4에 적힌 "최상위 data 배열" 구조와 다름 - 실기동 확인 결과 반영)
        header = raw.get("response", {}).get("header", {})
        result_code = header.get("resultCode")
        if result_code is not None and str(result_code) != "0":
            msg = header.get("resultMsg") or RESULT_CODE_MESSAGES.get(
                _to_int(result_code), "알 수 없는 에러"
            )
            print(f"[API 에러] resultCode={result_code}, resultMsg={msg}", file=sys.stderr)
            sys.exit(1)  # API 레벨 에러는 재시도해도 소용없으므로 즉시 종료

        return raw

    # 재시도를 모두 소진한 경우
    print(
        f"[실패] 페이지 {page_no}: {MAX_RETRIES}회 재시도 후에도 연결 실패 - {last_error}",
        file=sys.stderr,
    )
    raise last_error


def parse_record(item):
    return {
        "iso_code": item.get("country_iso_alp2"),
        "name": item.get("country_eng_nm"),
        "name_kr": item.get("country_nm"),
        "advisory_level": _to_int(item.get("alarm_lvl")),
        "advisory_text": item.get("remark") or None,
        "region_type": item.get("region_ty") or None,
        "continent_code": item.get("continent_cd"),
        "continent": item.get("continent_eng_nm"),
        "continent_kr": item.get("continent_nm"),
        "danger_map_url": item.get("dang_map_download_url") or None,
        "flag_url": item.get("flag_download_url") or None,
        "map_url": item.get("map_download_url") or None,
        "last_updated": item.get("written_dt") or None,  # null인 경우 잦음 (실기동 확인)
        "country_idx": item.get("org_country_idx"),
        "risk_score": None,
    }


def _to_int(v):
    if v in (None, ""):
        return None
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
        body = raw.get("response", {}).get("body", {})
        items = body.get("items", {})
        if isinstance(items, dict):
            items = items.get("item", [])
        if isinstance(items, dict):  # 결과가 1건이면 item이 dict로만 오는 경우 방어
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
