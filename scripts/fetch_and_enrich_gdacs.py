import json
import re
import time
import urllib.request
import urllib.error
from datetime import datetime, timedelta, timezone
from collections import Counter

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json",
}
DAYS_BACK = 30  # 최근 N일치 이벤트만 표시 (ENABLE_DATE_FILTER=True일 때만 적용)
ENABLE_DATE_FILTER = False  # False면 날짜 제한 없이 전부 가져옴 (디버깅/검증용)

# 🌟 [변경] 여러 타입을 한 번에 묶어서 요청하지 않고, 타입별로 개별 요청한다.
# GDACS SEARCH API의 pagenumber 파라미터가 신뢰할 수 없게 동작하는 것을 확인했음
# (페이지2가 페이지1과 동일한 내용을 반환하거나 204/빈 응답을 반환하는 경우가 있음).
# 여러 타입을 한 쿼리로 묶으면, 이 불안정한 페이지네이션 때문에 일부 타입이
# 통째로 누락될 위험이 있으므로 타입별로 쪼개서 요청해 위험을 분산시킨다.
EVENT_TYPES = ["EQ", "TC", "FL", "VO", "WF", "DR", "TS"]

BASE_URL_TEMPLATE = (
    "https://www.gdacs.org/gdacsapi/api/events/geteventlist/SEARCH"
    "?eventlist={eventtype}&alertlevel=green%3Borange%3Bred"
)

# 🌟 [추가] 페이지 요청 실패(비-200 또는 빈 body) 시 재시도 설정
PAGE_RETRY_COUNT = 3        # 페이지당 최대 재시도 횟수
PAGE_RETRY_DELAY_SEC = 2.0  # 재시도 간 대기 시간(초)

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

# True면 iscurrent=true(진행중)인 이벤트만 표시, False면 전부 표시(검증용)
SHOW_ONLY_CURRENT = False


def feature_key(feat):
    props = feat.get("properties", {}) or {}
    etype = props.get("eventtype", "")
    eid = props.get("eventid", "")
    if etype or eid:
        return f"{etype}{eid}"
    return None


# ⭐ [핵심 수정] 재귀 대신 깊이 우선 탐색(Stack DFS)을 활용해 Stack Overflow 방지
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

        # [longitude, latitude] 형태의 리프 노드에 도달했는지 검사
        if len(curr) >= 2 and all(isinstance(x, (int, float)) for x in curr[:2]):
            pts.append((float(curr[0]), float(curr[1])))
        else:
            # 하위 리스트(폴리곤 등 복합 지오메트리 구조)가 있으면 스택에 주입
            stack.extend(curr)

    if not pts:
        return None

    lng = sum(p[0] for p in pts) / len(pts)
    lat = sum(p[1] for p in pts) / len(pts)
    return lng, lat


def fetch_json(url, timeout=15):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        print(f"  ⚠️ 상세정보 조회 실패: {url} ({e})")
        return None


def parse_gdacs_date(date_str):
    """GDACS 날짜 문자열(YYYY-MM-DDTHH:MM:SS)을 timezone-aware datetime으로 변환"""
    if not date_str:
        return None
    try:
        dt = datetime.fromisoformat(str(date_str).replace("Z", ""))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except ValueError:
        return None


# 🌟 [추가] 페이지 하나를 재시도 포함해서 가져오는 헬퍼 함수
def fetch_page_with_retry(url, retries=PAGE_RETRY_COUNT, delay=PAGE_RETRY_DELAY_SEC):
    """
    페이지 요청 시 비-200 또는 빈 body가 오면 최대 `retries`번까지 재시도.
    GDACS가 일시적으로 204/빈 응답을 주는 경우(레이트리밋, 서버 hiccup 등) 때문에
    실제로는 더 많은 페이지가 남아있는데도 조기 종료되는 문제를 막기 위함.
    반환값: (status, body) — 모든 재시도가 실패하면 마지막 시도의 (status, body) 반환
    """
    status, body = None, None
    for attempt in range(1, retries + 1):
        try:
            req = urllib.request.Request(url, headers=HEADERS)
            with urllib.request.urlopen(req, timeout=20) as resp:
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
    """
    🌟 [추가] 단일 타입(EQ/TC/FL/VO/WF/DR/TS)에 대해서만 GDACS SEARCH API를 호출.
    타입별로 결과 건수가 적어 페이지네이션이 거의 필요 없고, 설령 이 타입의
    요청이 전부 실패하더라도 다른 타입 데이터에는 영향을 주지 않는다.
    """
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

        # GDACS의 pagenumber가 신뢰할 수 없게 동작하는 걸 감안해,
        # 이번 페이지의 첫 항목이 이전 페이지의 첫 항목과 같으면
        # (즉 진짜 다음 페이지가 아니라 같은 내용을 다시 준 것이면) 중단
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
    """1단계: GDACS 이벤트 리스트 수집 및 파싱 (타입별 개별 요청 방식)"""
    all_features = []

    # 🌟 [변경] 타입 하나씩 순회하며 개별 수집
    for event_type in EVENT_TYPES:
        print(f"GDACS [{event_type}] 수집 시작")
        type_features = fetch_events_for_type(event_type)
        print(f"GDACS [{event_type}] {len(type_features)}건 수집 완료")
        all_features.extend(type_features)
        time.sleep(0.5)  # 타입별 요청 사이 짧은 대기 (레이트리밋 방지)

    # 원본 데이터의 타입별 분포를 로그로 남겨서, 특정 타입만 누락되는지 바로 확인 가능
    type_counts = Counter((f.get("properties", {}) or {}).get("eventtype", "?") for f in all_features)
    print(f"수집된 원본 이벤트 타입 분포: {dict(type_counts)}")

    # 파싱 진행
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

    print(f"총 {len(results)}건 재난 추출 완료 (스킵: {skipped_no_geom}좌표누락, {skipped_not_current}비활성, {skipped_duplicate}중복, {skipped_too_old}기간초과({DAYS_BACK}일))")

    # 🌟 [추가] 최종 결과의 타입별 분포도 로그로 남겨 필터링 단계에서 특정 타입이
    # 전부 사라지는 문제가 있는지 바로 확인할 수 있게 함
    result_type_counts = Counter(r.get("eventtype", "?") for r in results)
    print(f"최종 결과 타입별 분포: {dict(result_type_counts)}")

    return results


def enrich_disasters(results):
    """2단계: 각 이벤트 상세정보 조회 및 보강"""
    results.sort(key=lambda r: SEVERITY_ORDER.get(str(r.get("alert_level", "green")).lower(), 3))

    enrich_count = 0
    enriched_ok = 0

    for r in results:
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
            continue

        detail_url = f"https://www.gdacs.org/gdacsapi/api/events/geteventdata?eventtype={eventtype}&eventid={eventid}"
        detail = fetch_json(detail_url)
        enrich_count += 1
        time.sleep(0.6)

        if not detail:
            continue

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

        # 지진 상세 정보 (magnitude/depth/노출인구는 eventtype이 EQ일 때만 존재)
        r["magnitude"] = eq_details.get("magnitude")
        r["depth_km"] = eq_details.get("depth")
        r["event_date_local"] = eq_details.get("episodedatelocal")
        r["exposed_population"] = eq_details.get("rapidpop")
        r["exposed_population_description"] = eq_details.get("rapidpopdescription")

        enriched_ok += 1

    print(f"상세정보 API 호출 {enrich_count}건 중 {enriched_ok}건 보강 완료")
    return results


def main():
    results = fetch_disaster_list()
    results = enrich_disasters(results)

    with open("data/realtime_disasters.json", "w", encoding="utf-8") as f:
        json.dump({"status": "success", "data": results}, f, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    main()
