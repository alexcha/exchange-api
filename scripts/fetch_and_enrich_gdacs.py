import json
import re
import sys
import time
import urllib.request
import urllib.error

# ==========================================
# 1. 전역 설정 및 맵핑 정의
# ==========================================
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json",
}

BASE_URL = (
    "https://www.gdacs.org/gdacsapi/api/events/geteventlist/SEARCH"
    "?eventlist=EQ;TC;FL;VO;WF;DR;TS&alertlevel=green;orange;red"
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

SEVERITY_ORDER = {"red": 0, "orange": 1, "green": 2}

# 재난 타입 영문명
EVENT_TYPE_NAME = {
    "EQ": "earthquake", 
    "TC": "tropical cyclone", 
    "FL": "flood",
    "WF": "wildfire", 
    "VO": "volcanic eruption", 
    "DR": "drought", 
    "TS": "tsunami",
}

# 어색한 'low/medium/significant humanitarian impact' 대신 쓰일 구호 리포트용 자연스러운 표현
IMPACT_LEVEL_DESC = {
    "green": "minimal",          # 아주 미미한 수준
    "orange": "moderate",        # 중간 및 부분적인 수준
    "red": "highly severe",      # 매우 극심한 수준
}

# 분석 기준 영문명
IMPACT_BASIS = {
    "EQ": "magnitude", 
    "TC": "wind speed", 
    "FL": "flood extent",
    "WF": "affected area", 
    "VO": "eruption size", 
    "DR": "drought severity", 
    "TS": "wave height",
}


# ==========================================
# 2. 유틸리티 함수
# ==========================================
def fetch_json(url, timeout=15):
    try:
        req = urllib.request.Request(url, headers=HEADERS)
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        print(f"  ⚠️ 상세정보 조회 실패: {url} ({e})")
        return None


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
    return sum(p[0] for p in pts) / len(pts), sum(p[1] for p in pts) / len(pts)


def clean_html(raw_html):
    if not raw_html:
        return ""
    cleaned = re.sub(r'<[^>]*>', ' ', str(raw_html))
    cleaned = cleaned.replace("&nbsp;", " ").replace("&amp;", "&").replace("&quot;", '"')
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()
    return cleaned


# ==========================================
# 3. 메인 파이프라인 실행 엔진
# ==========================================
def main():
    print("🚀 GDACS 실시간 재난 목록 데이터 수집을 시작합니다...")
    
    all_features = []
    seen_keys = set()
    MAX_PAGES = 10  
    prev_first_key = None

    for page in range(1, MAX_PAGES + 1):
        url = f"{BASE_URL}&pagenumber={page}"
        try:
            req = urllib.request.Request(url, headers=HEADERS)
            with urllib.request.urlopen(req, timeout=20) as resp:
                status = resp.status
                body = resp.read()
        except Exception as e:
            print(f"⚠️ GDACS 페이지 {page} 요청 실패: {e}")
            break

        if status != 200 or not body:
            break

        try:
            page_json = json.loads(body)
        except Exception as e:
            print(f"⚠️ 페이지 {page} JSON 파싱 실패: {e}")
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
        desc_clean = clean_html(desc)

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
            "report_description": desc_clean,
            "impact_description": "",      # 자연스럽게 개선된 영문 분석 요약이 들어갈 자리
        })

    print(f"✅ 1차 추출 완료: 총 {len(results)}건")

    if not results:
        print("❌ 추출된 재난 정보가 없습니다.")
        sys.exit(1)

    results.sort(key=lambda r: SEVERITY_ORDER.get(str(r.get("alert_level", "green")).lower(), 3))

    enrich_count = 0
    enriched_ok = 0

    print("🔍 각 재난 상세 API 보강 프로세스를 시작합니다...")
    for r in results:
        eventtype = r.get("eventtype") or r.get("event_type")
        eventid = r.get("eventid")

        if not eventid:
            m = re.search(r"(\d+)$", str(r.get("gdacs_id", "")))
            if m:
                eventid = m.group(1)

        r_type = (eventtype or "").upper()
        r_alert = str(r.get("alert_level", "green")).lower()

        # 🌟 [영문 리포트 자연스러운 의역 템플릿]
        # 직역 투의 투박한 표현 대신, 전문 구호 기관 보고서에 등장하는 부드럽고 매끄러운 영문 구조로 변경했습니다.
        disaster_name = EVENT_TYPE_NAME.get(r_type, 'disaster event')
        basis_factor = IMPACT_BASIS.get(r_type, 'severity')
        impact_level = IMPACT_DESC = IMPACT_LEVEL_DESC.get(r_alert, 'minimal')

        natural_summary_en = (
            f"Based on the analyzed {basis_factor} of this {disaster_name}, along with the local "
            f"population exposure and vulnerability index, the overall impact on human lives "
            f"and local communities is expected to be {impact_level}."
        )

        r["impact_description"] = natural_summary_en

        if not eventtype or not eventid:
            continue

        detail_url = f"https://www.gdacs.org/gdacsapi/api/events/geteventdata?eventtype={eventtype}&eventid={eventid}"
        detail = fetch_json(detail_url)
        enrich_count += 1
        time.sleep(0.5)

        if not detail:
            continue

        props = detail.get("properties", detail) or {}

        # 리포트 본문 원본 매핑
        api_desc = props.get("htmldescription") or props.get("description") or props.get("summary") or ""
        api_desc_cleaned = clean_html(api_desc)

        if api_desc_cleaned:
            r["report_description"] = api_desc_cleaned

        sendai = props.get("sendai") or []
        severity = props.get("severitydata") or {}
        images = props.get("images") or {}

        deaths, displaced, missing = 0, 0, 0
        deaths_found = displaced_found = missing_found = False
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
        r["overview_map_url"] = images.get("overviewmap") or images.get("overviewmap_cached")
        r["report_detail_url"] = detail_url

        enriched_ok += 1

    print(f"📊 상세정보 API 호출 {enrich_count}건 중 {enriched_ok}건 보강 완료")

    with open("data/realtime_disasters.json", "w", encoding="utf-8") as f:
        json.dump({"status": "success", "data": results}, f, ensure_ascii=False, indent=2)
    print("🎉 수집 가공 및 영문 자연화 처리가 성공적으로 완료되었습니다!")


if __name__ == "__main__":
    main()
