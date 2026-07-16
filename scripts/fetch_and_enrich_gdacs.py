import json
import re
import sys
import time
import urllib.request
import urllib.error
from collections import defaultdict

# ==========================================
# 1. 전역 설정 및 맵핑 정의
# ==========================================
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json",
}

# 전염병(EP)을 수집 목록에 추가하여 꼼꼼한 모니터링 대응
BASE_URL = (
    "https://www.gdacs.org/gdacsapi/api/events/geteventlist/SEARCH"
    "?eventlist=EQ;TC;FL;VO;WF;DR;TS;EP&alertlevel=green;orange;red"
)

GDACS_COUNTRY_MAP = {
    "republic of korea": "South Korea",
    "korea, republic of": "South Korea",
    "united states of america": "United States",
    "us": "United States",
    "viet nam": "Vietnam",
    "russian federation": "Russia",
    "syrian arab republic": "Syria",
    "turkiye": "Turkey"
}

# 보강 및 리포트 작성용 매핑 (EP 포함)
EVENT_TYPE_NAME = {
    "EQ": "earthquake", 
    "TC": "tropical cyclone", 
    "FL": "flood",
    "WF": "wildfire", 
    "VO": "volcanic event", 
    "DR": "drought",
    "TS": "tsunami",
    "EP": "epidemic"
}

IMPACT_LEVEL = {
    "green": "low", 
    "orange": "medium", 
    "red": "significant"
}

IMPACT_BASIS = {
    "EQ": "the magnitude", 
    "TC": "the wind speed", 
    "FL": "the flood extent",
    "WF": "the affected area", 
    "VO": "the eruption size", 
    "DR": "the drought severity",
    "TS": "the wave height",
    "EP": "the outbreak scale"
}

SEVERITY_ORDER = {"red": 0, "orange": 1, "green": 2}


# ==========================================
# 2. 유틸리티 함수
# ==========================================
def fetch_json(url, timeout=15, max_retries=3):
    """지정한 URL에서 JSON 데이터를 안전하게 수집하며, 실패 시 재시도합니다."""
    for attempt in range(1, max_retries + 1):
        try:
            req = urllib.request.Request(url, headers=HEADERS)
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except Exception as e:
            print(f"  ⚠️ API 데이터 조회 실패 (시도 {attempt}/{max_retries}): {url} ({e})")
            if attempt < max_retries:
                time.sleep(2 * attempt)  # 재시도 대기 시간 점진적 증가
            else:
                return None


def feature_key(feat):
    """수집 시 중복 판정을 위해 고유 키를 추출합니다."""
    props = feat.get("properties", {}) or {}
    etype = props.get("eventtype", "")
    eid = props.get("eventid", "")
    if etype or eid:
        return f"{etype}{eid}"
    return None


def get_centroid(geom):
    """DFS 스택을 활용하여 메모리 누수나 Stack Overflow 없이 안전하게 무게중심 좌표를 구합니다."""
    if not isinstance(geom, dict):
        return None
    coords = geom.get("coordinates")
    if coords is None:
        return None

    pts = []
    stack = [coords]
    
    while stack:
        curr = stack.pop()
        if not isinstance(curr, list) or not curr:
            continue
        
        # [longitude, latitude] 단일 매핑 체크
        if len(curr) >= 2 and all(isinstance(x, (int, float)) for x in curr[:2]):
            pts.append((float(curr[0]), float(curr[1])))
        else:
            # 하위 리스트(멀티 폴리곤 등의 레이어) 분해 후 스택에 보관
            stack.extend(curr)

    if not pts:
        return None

    lng = sum(p[0] for p in pts) / len(pts)
    lat = sum(p[1] for p in pts) / len(pts)
    return lng, lat


# ==========================================
# 3. 메인 파이프라인 실행 엔진
# ==========================================
def main():
    print("🚀 GDACS 실시간 재난 목록 데이터 수집을 시작합니다...")
    
    all_features = []
    seen_keys = set()
    MAX_PAGES = 10  
    prev_first_key = None
    fetch_error_occurred = False

    # --- [1단계] 목록 조회 루프 ---
    for page in range(1, MAX_PAGES + 1):
        url = f"{BASE_URL}&pagenumber={page}"
        
        status = None
        body = None
        # 페이지 조회 단계에도 재시도(Retry) 적용
        for attempt in range(1, 4):
            try:
                req = urllib.request.Request(url, headers=HEADERS)
                with urllib.request.urlopen(req, timeout=20) as resp:
                    status = resp.status
                    body = resp.read()
                break
            except Exception as e:
                print(f"⚠️ GDACS 페이지 {page} 요청 실패 (시도 {attempt}/3): {e}")
                if attempt < 3:
                    time.sleep(2 * attempt)
                else:
                    fetch_error_occurred = True

        # 결국 응답을 받아오지 못한 경우 수집 중단
        if fetch_error_occurred or status != 200 or not body:
            print(f"❌ GDACS 페이지 {page}에서 데이터를 정상적으로 받아오지 못했습니다. 파이프라인을 중단합니다.")
            sys.exit(1)

        print(f"GDACS 페이지 {page} 응답 HTTP 상태코드: {status}, 크기: {len(body)} bytes")

        try:
            page_json = json.loads(body)
        except Exception as e:
            print(f"❌ 페이지 {page} JSON 파싱 실패: {e}. 기존 데이터 유실 방지를 위해 중단합니다.")
            sys.exit(1)

        page_features = page_json.get("features")
        if page_features is None:
            for alt_key in ("data", "results", "events", "FeatureCollection"):
                candidate = page_json.get(alt_key)
                if isinstance(candidate, dict):
                    candidate = candidate.get("features")
                if isinstance(candidate, list):
                    page_features = candidate
                    break

        if not page_features:
            break

        current_first_key = feature_key(page_features[0]) if page_features else None
        if page > 1 and current_first_key is not None and current_first_key == prev_first_key:
            break
        prev_first_key = current_first_key

        new_in_page = 0
        for feat in page_features:
            key = feature_key(feat)
            if key is not None and key in seen_keys:
                continue
            if key is not None:
                seen_keys.add(key)
            all_features.append(feat)
            new_in_page += 1

        if new_in_page == 0:
            break

    # --- 수집된 기초 원본 데이터 유무 검증 ---
    if not all_features:
        print("❌ 수집된 원본 데이터(features)가 전혀 없습니다. 기존 데이터를 유지하기 위해 파이프라인을 중단합니다.")
        sys.exit(1)

    # --- [2단계] 기본 파싱 및 1차 가공 ---
    results = []
    skipped_not_current = 0
    skipped_duplicate = 0
    skipped_no_geom = 0
    seen_result_keys = set()

    for feat in all_features:
        props = feat.get("properties", {}) or {}
        geom = feat.get("geometry", {}) or {}

        event_type_val = props.get("eventtype", "")
        event_id_val = props.get("eventid", "")

        # 현재 진행중 필터링 (가뭄 DR은 예외 유지)
        is_current = str(props.get("iscurrent", "")).strip().lower()
        if is_current != "true" and event_type_val != "DR":
            skipped_not_current += 1
            continue

        if event_type_val and event_id_val:
            result_key = f"{event_type_val}{event_id_val}"
            if result_key in seen_result_keys:
                skipped_duplicate += 1
                continue
            seen_result_keys.add(result_key)

        centroid = get_centroid(geom)
        if centroid is None:
            skipped_no_geom += 1
            continue
        lng, lat = centroid

        if not (-180 <= lng <= 180 and -90 <= lat <= 90):
            skipped_no_geom += 1
            continue

        raw_country = (props.get("country") or "").strip()
        clean_country = GDACS_COUNTRY_MAP.get(raw_country.lower(), raw_country)

        event_name = props.get("eventname") or props.get("name") or props.get("eventtype", "Disaster")
        title = f"{event_name} - {clean_country}" if clean_country else event_name

        desc = props.get("description") or props.get("htmldescription") or ""
        desc_clean = re.sub(r'<[^>]*>', '', desc).strip()

        if not desc_clean:
            severity_str = (props.get("alertlevel") or "unknown").upper()
            desc_clean = f"A {severity_str} level {event_name} event has been detected near {clean_country or 'coordinates'}: {lat}, {lng}."

        api_report_url = (props.get("url") or {}).get("report", "") if isinstance(props.get("url"), dict) else ""
        if api_report_url:
            report_url = api_report_url
        elif event_type_val and event_id_val:
            report_url = f"https://www.gdacs.org/report.aspx?eventtype={event_type_val}&eventid={event_id_val}"
        else:
            report_url = ""

        results.append({
            "latitude": lat,
            "longitude": lng,
            "title": title,
            "summary": desc_clean,
            "country": clean_country,
            "iso3": props.get("iso3", ""),
            "gdacs_id": f"{event_type_val}{event_id_val}",
            "eventtype": event_type_val,
            "eventid": event_id_val,
            "activation_number": props.get("glide"),
            "event_type": event_type_val,
            "alert_level": props.get("alertlevel", "green"),
            "alert_score": props.get("alertscore", 0),
            "report_url": report_url,
            "last_updated": props.get("todate") or props.get("fromdate") or "",
            "severity": props.get("alertlevel", "green"),
            "is_current": True,
        })

    print(f"✅ 기본 데이터 가공 완료: 총 {len(results)}건 (스킵: {skipped_no_geom}좌표누락, {skipped_not_current}비활성, {skipped_duplicate}중복)")

    # --- 가공된 최종 재난 리스트 유무 최종 검증 ---
    # 일시적으로 "현재 활성화된 재난"이 지구상에 한 건도 없을 수는 있지만, 
    # API 오류로 인한 사이드 이펙트를 차단하기 위해 0건일 때 파일 덮어쓰기를 방지하고 프로세스를 중단시킵니다.
    if not results:
        print("❌ 필터링 및 가공 결과가 0건입니다. 기존 정상 데이터를 보존하기 위해 파일을 쓰지 않고 중단합니다.")
        sys.exit(1)

    # --- [3단계] 심각도 순 정렬 및 상세 정보 API 호출 보강 ---
    results.sort(key=lambda r: SEVERITY_ORDER.get(str(r.get("alert_level", "green")).lower(), 3))

    enrich_count = 0
    enriched_ok = 0

    print("🔍 각 재난 상세 API 보강 프로세스를 시작합니다...")
    for r in results:
        r_eventtype = r.get("eventtype") or r.get("event_type") or ""
        r_alertlevel = str(r.get("alert_level", "green")).lower()
        
        # 1차 서술 설명 작성
        r["report_description"] = (
            f"This {EVENT_TYPE_NAME.get(r_eventtype, 'event')} can have a "
            f"{IMPACT_LEVEL.get(r_alertlevel, 'unknown')} humanitarian impact "
            f"based on {IMPACT_BASIS.get(r_eventtype, 'the severity')} and the "
            f"affected population and their vulnerability."
        )

        eventtype = r.get("eventtype") or r.get("event_type")
        eventid = r.get("eventid")

        if not eventid:
            m = re.search(r"(\d+)$", str(r.get("gdacs_id", "")))
            if m:
                eventid = m.group(1)

        if not eventtype or not eventid:
            continue

        detail_url = f"https://www.gdacs.org/gdacsapi/api/events/geteventdata?eventtype={eventtype}&eventid={eventid}"
        # fetch_json 유틸리티 자체에서 재시도를 거쳐 수집함
        detail = fetch_json(detail_url)
        enrich_count += 1
        time.sleep(0.6)  # GDACS 트래픽 차단 방지용 안전 지연

        if not detail:
            continue

        props = detail.get("properties", detail) or {}
        sendai = props.get("sendai") or []
        severity = props.get("severitydata") or {}
        images = props.get("images") or {}

        deaths = 0
        displaced = 0
        missing = 0
        deaths_found = False
        displaced_found = False
        missing_found = False
        sendai_details = []

        for s in sendai:
            name = (s.get("sendainame") or "").lower()
            try:
                val = int(re.sub(r"[^\d]", "", str(s.get("sendaivalue", "0"))) or 0)
            except ValueError:
                val = 0

            if "death" in name:
                deaths += val
                deaths_found = True
            elif "displaced" in name or "evacuat" in name:
                displaced += val
                displaced_found = True
            elif "missing" in name:
                missing += val
                missing_found = True

            sendai_details.append({
                "type": s.get("sendaitype"),
                "name": s.get("sendainame"),
                "value": s.get("sendaivalue"),
                "region": s.get("region"),
                "description": s.get("description"),
                "date": s.get("dateinsert"),
                "latest": s.get("latest"),
            })

        r["deaths"] = deaths if deaths_found else None
        r["displaced"] = displaced if displaced_found else None
        r["missing"] = missing if missing_found else None

        r["severity_text"] = severity.get("severitytext")
        r["impact_history"] = sendai_details
        r["impact_description"] = (sendai_details[-1]["description"][:300] if sendai_details else None)
        r["overview_map_url"] = images.get("overviewmap") or images.get("overviewmap_cached")
        r["report_detail_url"] = detail_url

        enriched_ok += 1

    print(f"📊 상세정보 API 호출 {enrich_count}건 중 {enriched_ok}건 완벽 보강 완료")
    
    # --- [4단계] JSON 결과 저장 ---
    # 안전장치들을 모두 거치고 리스트가 완벽할 때만 최종적으로 파일을 업데이트합니다.
    with open("data/realtime_disasters.json", "w", encoding="utf-8") as f:
        json.dump({"status": "success", "data": results}, f, ensure_ascii=False, indent=2)
    print("🎉 모든 가공 데이터가 'data/realtime_disasters.json'에 안전하게 저장되었습니다.")


if __name__ == "__main__":
    main()
