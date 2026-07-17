import json
import re
import time
import urllib.request
import urllib.error
import sys
import os
from datetime import datetime, timedelta, timezone
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json",
}
DAYS_BACK = 30  # 최근 N일치 이벤트만 표시 (ENABLE_DATE_FILTER=True일 때만 적용)
ENABLE_DATE_FILTER = False  # False면 날짜 제한 없이 전부 가져옴 (디버깅/검증용)

# 각 재난 타입별 최대로 남길 개수 지정
MAX_ITEMS_PER_TYPE = 10

# 병렬 처리에 사용할 최대 스레드 수 (너무 높으면 GDACS 서버에서 차단당할 수 있으므로 8이 적당합니다)
MAX_WORKERS = 8

# GDACS SEARCH API 개별 요청 설정
EVENT_TYPES = ["EQ", "TC", "FL", "VO", "WF", "DR", "TS"]

BASE_URL_TEMPLATE = (
    "https://www.gdacs.org/gdacsapi/api/events/geteventlist/SEARCH"
    "?eventlist={eventtype}&alertlevel=green%3Borange%3Bred"
)

# 페이지 요청 실패 시 재시도 설정 (타임아웃 방지 강화)
PAGE_RETRY_COUNT = 3        
PAGE_RETRY_DELAY_SEC = 2.0  

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

EVENT_TYPE_NAME = {
    "EQ": "earthquake", "TC": "tropical cyclone", "FL": "flood",
    "WF": "wildfire", "VO": "volcanic event", "DR": "drought",
    "TS": "tsunami",
}
IMPACT_LEVEL = {"green": "low", "orange": "medium", "red": "significant"}
IMPACT_BASIS = {
    "EQ": "the magnitude", "TC": "the wind speed", "FL": "the flood extent",
    "WF": "the affected area", "VO": "the eruption size", "DR": "the drought severity",
    "TS": "the wave height",
}

SEVERITY_ORDER = {"red": 0, "orange": 1, "green": 2}

SHOW_ONLY_CURRENT = False


def feature_key(feat):
    props = feat.get("properties", {}) or {}
    etype = props.get("eventtype", "")
    eid = props.get("eventid", "")
    if etype or eid:
        return f"{etype}{eid}"
    return None


def get_centroid(geom):
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

        if len(curr) >= 2 and all(isinstance(x, (int, float)) for x in curr[:2]):
            pts.append((float(curr[0]), float(curr[1])))
        else:
            stack.extend(curr)

    if not pts:
        return None

    lng = sum(p[0] for p in pts) / len(pts)
    lat = sum(p[1] for p in pts) / len(pts)
    return lng, lat


# 타임아웃을 5초로 줄여 병목을 원천 방지합니다.
def fetch_json(url, timeout=5):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        print(f"  ⚠️ 상세정보 조회 실패: {url} ({e})")
        return None


def parse_gdacs_date(date_str):
    if not date_str:
        return None
    try:
        dt = datetime.fromisoformat(str(date_str).replace("Z", ""))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except ValueError:
        return None


def fetch_page_with_retry(url, retries=PAGE_RETRY_COUNT, delay=PAGE_RETRY_DELAY_SEC):
    status, body = None, None
    for attempt in range(1, retries + 1):
        try:
            req = urllib.request.Request(url, headers=HEADERS)
            # 목록 요청 타임아웃 15초로 최적화
            with urllib.request.urlopen(req, timeout=15) as resp:
                status = resp.status
                body = resp.read()
        except Exception as e:
            print(f"    ⚠️ 페이지 요청 실패 (시도 {attempt}/{retries}): {e}")
            status, body = None, None

        if status == 200 and body:
            return status, body

        if attempt < retries:
            print(f"    ↻ 응답 이상 (status={status}, size={len(body) if body else 0}) "
                  f"- {delay}초 후 재시도 ({attempt}/{retries})")
            time.sleep(delay)

    return status, body


def fetch_events_for_type(event_type, max_pages=5):
    base_url = BASE_URL_TEMPLATE.format(eventtype=event_type)
    all_features = []
    seen_keys = set()
    prev_first_key = None

    for page in range(1, max_pages + 1):
        url = f"{base_url}&pagenumber={page}"
        status, body = fetch_page_with_retry(url)

        print(f"  [{event_type}] 페이지 {page} 응답 HTTP 상태코드: {status}, "
              f"크기: {len(body) if body else 0} bytes")

        if status != 200 or not body:
            print(f"  ⛔ [{event_type}] 페이지 {page} 재시도 {PAGE_RETRY_COUNT}회 모두 실패 - 이 타입 수집 중단")
            break

        try:
            page_json = json.loads(body)
        except Exception as e:
            print(f"  [{event_type}] 페이지 {page} JSON 파싱 실패: {e}")
            break

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

        current_first_key = feature_key(page_features[0])
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

    return all_features


def fetch_disaster_list():
    all_features = []

    print("==================================================")
    print("🚀 실시간 재난 데이터 수집 가동 (GDACS API)")
    print("==================================================")

    for idx, event_type in enumerate(EVENT_TYPES, 1):
        print(f"\n📡 [{idx}/{len(EVENT_TYPES)}] GDACS [{event_type}] 수집 시작")
        type_features = fetch_events_for_type(event_type)
        print(f"  ✅ [{event_type}] {len(type_features)}건 수집 완료")
        all_features.extend(type_features)
        time.sleep(0.3)  # 레이트리밋 방지 대기 시간 소폭 단축

    type_counts = Counter((f.get("properties", {}) or {}).get("eventtype", "?") for f in all_features)
    print(f"\n📊 수집된 원본 이벤트 타입 분포: {dict(type_counts)}")

    results = []
    skipped_not_current = 0
    skipped_duplicate = 0
    skipped_no_geom = 0
    skipped_too_old = 0
    seen_result_keys = set()
    cutoff_date = datetime.now(timezone.utc) - timedelta(days=DAYS_BACK)

    for feat in all_features:
        props = feat.get("properties", {}) or {}
        geom = feat.get("geometry", {}) or {}

        event_type_val = props.get("eventtype", "")
        event_id_val = props.get("eventid", "")

        is_current = str(props.get("iscurrent", "")).strip().lower()
        if SHOW_ONLY_CURRENT and is_current != "true" and event_type_val != "DR":
            skipped_not_current += 1
            continue

        event_date = parse_gdacs_date(props.get("todate")) or parse_gdacs_date(props.get("fromdate"))
        if ENABLE_DATE_FILTER and event_date is not None and event_date < cutoff_date:
            skipped_too_old += 1
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
            "is_current": is_current == "true",
        })

    print(f"\n⚙️  총 {len(results)}건 재난 추출 완료 (스킵: {skipped_no_geom}좌표누락, {skipped_not_current}비활성, {skipped_duplicate}중복, {skipped_too_old}기간초과)")

    print("\n✂️  각 항목(타입)별 최신 10건으로 제한 프로세스 시작")
    categorized = {etype: [] for etype in EVENT_TYPES}
    
    for r in results:
        etype = r.get("eventtype")
        if etype in categorized:
            categorized[etype].append(r)
            
    filtered_results = []
    for etype, items in categorized.items():
        def _get_date_key(x):
            dt = parse_gdacs_date(x.get("last_updated"))
            return dt or datetime.min.replace(tzinfo=timezone.utc)
            
        items.sort(key=_get_date_key, reverse=True)
        trimmed_items = items[:MAX_ITEMS_PER_TYPE]
        filtered_results.extend(trimmed_items)
        print(f"  • [{etype}] 총 {len(items)}건 중 최신 {len(trimmed_items)}건만 유지 (초과 {max(0, len(items) - 10)}건 삭제)")

    final_counts = Counter(r.get("eventtype") for r in filtered_results)
    print(f"👉 필터링 후 최종 결과 타입별 분포: {dict(final_counts)}")
    print(f"👉 총 수집 대상 목록 수: {len(filtered_results)}건")

    return filtered_results


# 개별 항목 상세 정보(Enrich)를 가져오는 스레드용 작업 함수
def process_single_enrich(r):
    r_eventtype = r.get("eventtype") or r.get("event_type") or ""
    r_alertlevel = str(r.get("alert_level", "green")).lower()
    r["report_description"] = (
        f"This {EVENT_TYPE_NAME.get(r_eventtype, 'event')} could have a "
        f"{IMPACT_LEVEL.get(r_alertlevel, 'unknown')} impact on affected communities, "
        f"based on {IMPACT_BASIS.get(r_eventtype, 'the severity')} and the "
        f"exposure and vulnerability of the population nearby."
    )

    eventtype = r.get("eventtype") or r.get("event_type")
    eventid = r.get("eventid")

    if not eventid:
        m = re.search(r"(\d+)$", str(r.get("gdacs_id", "")))
        if m:
            eventid = m.group(1)

    if not eventtype or not eventid:
        return r, False

    detail_url = f"https://www.gdacs.org/gdacsapi/api/events/geteventdata?eventtype={eventtype}&eventid={eventid}"
    detail = fetch_json(detail_url)

    if not detail:
        return r, False

    props = detail.get("properties", detail) or {}
    sendai = props.get("sendai") or []
    severity = props.get("severitydata") or {}
    images = props.get("images") or {}
    eq_details = props.get("earthquakedetails") or {}

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

    r["magnitude"] = eq_details.get("magnitude")
    r["depth_km"] = eq_details.get("depth")
    r["event_date_local"] = eq_details.get("episodedatelocal")
    r["exposed_population"] = eq_details.get("rapidpop")
    r["exposed_population_description"] = eq_details.get("rapidpopdescription")

    return r, True


def enrich_disasters_parallel(results):
    """2단계: 병렬(Thread) 처리를 통한 각 이벤트 상세정보 조회 및 보강"""
    results.sort(key=lambda r: SEVERITY_ORDER.get(str(r.get("alert_level", "green")).lower(), 3))

    print(f"\n🔄 [병렬 스레드 적용] 상세 정보(Enrich) 수집 시작... (총 {len(results)}개 대상, Workers: {MAX_WORKERS})")
    
    enriched_results = []
    enriched_ok = 0
    total_count = len(results)

    # ThreadPoolExecutor를 사용한 비동기 병렬 HTTP 요청 수행
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_single_enrich, r): r for r in results}
        
        for idx, future in enumerate(as_completed(futures), 1):
            updated_record, success = future.result()
            enriched_results.append(updated_record)
            if success:
                enriched_ok += 1
            
            # 실시간 진행 상황 콘솔 출력
            etype = updated_record.get("eventtype", "?")
            eid = updated_record.get("eventid", "?")
            status_text = "성공" if success else "실패"
            print(f"  ⚡ [{idx}/{total_count}] 완료: {etype}{eid} ({status_text})")

    print(f"\n🎉 상세정보 API 호출 {total_count}건 중 {enriched_ok}건 완벽 보강 완료!")
    return enriched_results


def main():
    results = fetch_disaster_list()
    
    if not results:
        print("\n🚨 [위험] 수집 및 필터링 완료된 데이터가 총 0건입니다.")
        print("💡 API 서버 장애 혹은 네트워크 타임아웃으로 예상되며, 기존 JSON 데이터를 보호하기 위해 프로그램 쓰기를 건너뛰고 정상 안전 종료합니다.")
        sys.exit(0)

    # 수집 성능 향상을 위해 기존 동기 함수를 "병렬 함수"로 전면 대체
    results = enrich_disasters_parallel(results)

    # [보호 장치] 임시 파일 생성 후 안전 교체 (Atomic Write)
    temp_filepath = "data/realtime_disasters.json.tmp"
    final_filepath = "data/realtime_disasters.json"

    # 상위 디렉터리가 없을 경우 생성
    os.makedirs(os.path.dirname(final_filepath), exist_ok=True)

    try:
        # 1. 먼저 임시 파일(.tmp)에 온전히 씁니다.
        with open(temp_filepath, "w", encoding="utf-8") as f:
            json.dump({"status": "success", "data": results}, f, ensure_ascii=False, indent=2)
        
        # 2. 파일 쓰기가 에러 없이 완전하게 끝나면, 원자적(Atomic)으로 기존 파일을 덮어씁니다.
        # 이 작업은 OS 레벨에서 찰나의 순간에 처리되므로 도중에 프로세스가 종료되어도 파일이 잘리지 않습니다.
        os.replace(temp_filepath, final_filepath)
        print(f"\n💾 파일 원자적 저장 성공: '{final_filepath}' 업데이트 완료!")
    except Exception as e:
        print(f"\n❌ 파일 쓰기 오류 발생: {e}")
        if os.path.exists(temp_filepath):
            os.remove(temp_filepath)
        sys.exit(1)


if __name__ == "__main__":
    main()
