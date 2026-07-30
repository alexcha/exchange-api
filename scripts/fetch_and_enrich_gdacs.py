import json
import re
import time
import hashlib
import uuid
import urllib.request
import urllib.parse
import urllib.error
import sys
import os
from datetime import datetime, timedelta, timezone
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
import firebase_admin
from firebase_admin import credentials, messaging

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json",
}

# GDACS 공식 엔드포인트 목록 (Primary / Fallback)
GDACS_SEARCH_URL = "https://www.gdacs.org/gdacsapi/api/events/geteventlist/SEARCH"
GDACS_APP_URL = "https://www.gdacs.org/gdacsapi/api/events/geteventlist/events4app"

FETCH_DAYS_BACK = 30
MAX_WORKERS = 8
PAGE_RETRY_COUNT = 3
PAGE_RETRY_DELAY_SEC = 3.0  # 타임아웃 완화를 위해 재시도 대기시간 증가
API_TIMEOUT_SEC = 30       # 읽기 타임아웃을 30초로 연장

SENT_IDS_FILEPATH = "data/sent_disaster_ids.json"
PRUNE_AFTER_DAYS = 45

# GDACS 카테고리별 전용 리포트 페이지 경로 매핑
CATEGORY_PATH_MAP = {
    "TC": "Cyclones/report.aspx",
    "EQ": "Earthquakes/report_smpreliminary.aspx",
    "FL": "Floods/report.aspx",
    "VO": "Volcanoes/report.aspx",
    "WF": "Wildfires/report.aspx",
    "DR": "Droughts/report.aspx",
    "TS": "Tsunami/report.aspx"
}

EVENT_TYPE_MAX_AGE_DAYS_BY_TYPE = {
    "EQ": 7,    # 지진
    "WF": 7,    # 산불
    "TS": 7,    # 쓰나미
    "TC": 30,   # 태풍 / 열대저기압
    "FL": 14,   # 홍수
    "VO": 30,   # 화산
    "DR": 60,   # 가뭄
}
DEFAULT_EVENT_TYPE_MAX_AGE_DAYS = 14

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

ISO3_TO_ISO2 = {
    "AFG":"AF","ALA":"AX","ALB":"AL","DZA":"DZ","ASM":"AS","AND":"AD","AGO":"AO","AIA":"AI",
    "ATA":"AQ","ATG":"AG","ARG":"AR","ARM":"AM","ABW":"AW","AUS":"AU","AUT":"AT","AZE":"AZ",
    "BHS":"BS","BHR":"BH","BGD":"BD","BRB":"BB","BLR":"BY","BEL":"BE","BLZ":"BZ","BEN":"BJ",
    "BMU":"BM","BTN":"BT","BOL":"BO","BES":"BQ","BIH":"BA","BWA":"BW","BVT":"BV","BRA":"BR",
    "IOT":"IO","BRN":"BN","BGR":"BG","BFA":"BF","BDI":"BI","CPV":"CV","KHM":"KH","CMR":"CM",
    "CAN":"CA","CYM":"KY","CAF":"CF","TCD":"TD","CHL":"CL","CHN":"CN","CXR":"CX","CCK":"CC",
    "COL":"CO","COM":"KM","COD":"CD","COG":"CG","COK":"CK","CRI":"CR","CIV":"CI","HRV":"HR",
    "CUB":"CU","CUW":"CW","CYP":"CY","CZE":"CZ","DNK":"DK","DJI":"DJ","DMA":"DM","DOM":"DO",
    "ECU":"EC","EGY":"EG","SLV":"SV","GNQ":"GQ","ERI":"ER","EST":"EE","SWZ":"SZ","ETH":"ET",
    "FLK":"FK","FRO":"FO","FJI":"FJ","FIN":"FI","FRA":"FR","GUF":"GF","PYF":"PF","ATF":"TF",
    "GAB":"GA","GMB":"GM","GEO":"GE","DEU":"DE","GHA":"GH","GIB":"GI","GRC":"GR","GRL":"GL",
    "GRD":"GD","GLP":"GP","GUM":"GU","GTM":"GT","GGY":"GG","GIN":"GN","GNB":"GW","GUY":"GY",
    "HTI":"HT","HMD":"HM","VAT":"VA","HND":"HN","HKG":"HK","HUN":"HU","ISL":"IS","IND":"IN",
    "IDN":"ID","IRN":"IR","IRQ":"IQ","IRL":"IE","IMN":"IM","ISR":"IL","ITA":"IT","JAM":"JM",
    "JPN":"JP","JEY":"JE","JOR":"JO","KAZ":"KZ","KEN":"KE","KIR":"KI","PRK":"KP","KOR":"KR",
    "KWT":"KW","KGZ":"KG","LAO":"LA","LVA":"LV","LBN":"LB","LSO":"LS","LBR":"LR","LBY":"LY",
    "LIE":"LI","LTU":"LT","LUX":"LU","MAC":"MO","MDG":"MG","MWI":"MW","MYS":"MY","MDV":"MV",
    "MLI":"ML","MLT":"MT","MHL":"MH","MTQ":"MQ","MRT":"MR","MUS":"MU","MYT":"YT","MEX":"MX",
    "FSM":"FM","MDA":"MD","MCO":"MC","MNG":"MN","MNE":"ME","MSR":"MS","MAR":"MA","MOZ":"MZ",
    "MMR":"MM","NAM":"NA","NRU":"NR","NPL":"NP","NLD":"NL","NCL":"NC","NZL":"NZ","NIC":"NI",
    "NER":"NE","NGA":"NG","NIU":"NU","NFK":"NF","MKD":"MK","MNP":"MP","NOR":"NO","OMN":"OM",
    "PAK":"PK","PLW":"PW","PSE":"PS","PAN":"PA","PNG":"PG","PRY":"PY","PER":"PE","PHL":"PH",
    "PCN":"PN","POL":"PL","PRT":"PT","PRI":"PR","QAR":"QA","REU":"RE","ROU":"RO","RUS":"RU",
    "RWA":"RW","BLM":"BL","SHN":"SH","KNA":"KN","LCA":"LC","MAF":"MF","SPM":"PM","VCT":"VC",
    "WSM":"WS","SMR":"SM","STP":"ST","SAU":"SA","SEN":"SN","SRB":"RS","SYC":"SC","SLE":"SL",
    "SGP":"SG","SVK":"SX","SVN":"SI","SLB":"SB","SOM":"SO","ZAF":"ZA","SGS":"GS","SSD":"SS",
    "ESP":"ES","LKA":"LK","SDN":"SD","SUR":"SR","SJM":"SJ","SWE":"SE","CHE":"CH","SYR":"SY",
    "TWN":"TW","TJK":"TJ","TZA":"TZ","THA":"TH","TLS":"TL","TGO":"TG","TKL":"TK","TON":"TO",
    "TTO":"TT","TUN":"TN","TUR":"TR","TKM":"TM","TCA":"TC","TUV":"TV","UGA":"UG","UKR":"UA",
    "ARE":"AE","GBR":"GB","UMI":"UM","USA":"US","URY":"UY","UZB":"UZ","VUT":"VU","VEN":"VE",
    "VNM":"VN","VGB":"VG","VIR":"VI","WLF":"WF","ESH":"EH","YEM":"YE","ZMB":"ZM","ZWE":"ZW",
}


def iso3_to_iso2(iso3_val: str) -> str:
    iso3_val = (iso3_val or "").upper().strip()
    if iso3_val in ISO3_TO_ISO2:
        return ISO3_TO_ISO2[iso3_val]
    return iso3_val[:2] if len(iso3_val) == 3 else ""


def compute_disaster_fingerprint(r):
    parts = [
        str(r.get("alert_level", "")),
        str(r.get("alert_score", "")),
        str(r.get("title", "")),
        str(r.get("summary", ""))[:200],
        str(r.get("deaths")),
        str(r.get("displaced")),
        str(r.get("missing")),
        str(r.get("severity_text", "")),
        str(r.get("magnitude", "")),
        str(r.get("max_wind_speed_kmh", "")),
        str(r.get("flood_area_sqkm", "")),
    ]
    fingerprint_source = "|".join(parts)
    return hashlib.sha256(fingerprint_source.encode("utf-8")).hexdigest()


def init_firebase():
    if not firebase_admin._apps:
        cred_json_str = os.environ.get('FIREBASE_SERVICE_ACCOUNT_JSON')
        if not cred_json_str:
            print("⚠️  [FCM] FIREBASE_SERVICE_ACCOUNT_JSON 환경변수가 없어서 알림 발송을 건너뜁니다.")
            return False
        try:
            cred_json = json.loads(cred_json_str)
            cred = credentials.Certificate(cred_json)
            firebase_admin.initialize_app(cred)
            return True
        except Exception as e:
            print(f"❌ [FCM] 파이어베이스 라이브러리 초기화 중 오류: {e}")
            return False
    return True


def send_disaster_push(country_iso2, country_iso3, country_name, disaster_title, event_id, last_updated):
    topic_name = "all"

    if not last_updated:
        last_updated = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    elif not (last_updated.endswith('Z') or last_updated.endswith('z')):
        last_updated = str(last_updated).replace(" ", "T") + "Z"

    c_name = country_name if country_name else "Global"
    alarm_id = uuid.uuid4().hex

    try:
        message = messaging.Message(
            data={
                "type": "travel_risk",
                "alarm_id": alarm_id,
                "iso_code": str(country_iso2).upper(),
                "isoCode": str(country_iso2).upper(),
                "iso3": str(country_iso3).upper(),
                "country": c_name,
                "countries": json.dumps([c_name, str(country_iso2).upper(), str(country_iso3).upper()]),
                "last_updated": last_updated,
                "event_date": last_updated,
                "id": str(event_id) if event_id else "disaster_evt_999"
            },
            android=messaging.AndroidConfig(
                priority="high"
            ),
            topic=topic_name
        )
        response = messaging.send(message)
        print(f"  👉 [FCM 알림 방송 성공] 토픽 채널: {topic_name} / 대상 국가: {c_name}({country_iso2}) / 알람ID: {alarm_id} (전송 ID: {response})")
    except Exception as e:
        print(f"  ❌ [FCM 알림 방송 실패] 토픽 채널: {topic_name} / 에러 내용: {e}")


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


def fetch_json(url, timeout=10):
    try:
        req = urllib.request.Request(url, headers=HEADERS)
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


def fetch_events_from_gdacs_geojson(days_back=FETCH_DAYS_BACK, retries=PAGE_RETRY_COUNT, delay=PAGE_RETRY_DELAY_SEC):
    """
    SEARCH API 및 Fallback URL을 순차 시도하여 GeoJSON 데이터 수집
    """
    now_utc = datetime.now(timezone.utc)
    from_date = (now_utc - timedelta(days=days_back)).strftime("%Y-%m-%d")
    to_date = (now_utc + timedelta(days=1)).strftime("%Y-%m-%d")

    event_list = "EQ;TC;FL;VO;WF;DR;TS"
    params = {
        "eventlist": event_list,
        "fromdate": from_date,
        "todate": to_date
    }
    
    # 시도할 candidate URL 리스트
    target_urls = [
        f"{GDACS_SEARCH_URL}?{urllib.parse.urlencode(params)}",
        f"{GDACS_APP_URL}",
        f"{GDACS_SEARCH_URL}?eventlist={event_list}"
    ]

    print(f"📡 GDACS GeoJSON API 요청 범위: {from_date} ~ {to_date}")

    for url_idx, target_url in enumerate(target_urls, 1):
        print(f"  🌐 [시도 {url_idx}/{len(target_urls)}] 요청 URL: {target_url}")
        for attempt in range(1, retries + 1):
            try:
                req = urllib.request.Request(target_url, headers=HEADERS)
                # 타임아웃을 30초로 지정하여 연장
                with urllib.request.urlopen(req, timeout=API_TIMEOUT_SEC) as resp:
                    if resp.status == 200:
                        body = resp.read()
                        data = json.loads(body.decode("utf-8"))
                        features = data.get("features", [])
                        print(f"  ✅ [GeoJSON API] 데이터 수집 성공! (HTTP 200, Total Features: {len(features)})")
                        return features
            except Exception as e:
                print(f"  ⚠️ [GeoJSON API] 요청 실패 (시도 {attempt}/{retries}): {e}")

            if attempt < retries:
                time.sleep(delay)

    return []


def fetch_disaster_list():
    print("==================================================")
    print("🚀 실시간 재난 데이터 수집 가동 (GDACS GeoJSON API)")
    print("==================================================")

    all_features = fetch_events_from_gdacs_geojson(days_back=FETCH_DAYS_BACK)
    type_counts = Counter((f.get("properties", {}) or {}).get("eventtype", "?") for f in all_features)
    print(f"\n📊 수집된 원본 이벤트 타입 분포: {dict(type_counts)}")

    results = []
    skipped_duplicate = 0
    skipped_no_geom = 0
    seen_result_keys = set()
    now_utc = datetime.now(timezone.utc)

    for feat in all_features:
        props = feat.get("properties", {}) or {}
        geom = feat.get("geometry", {}) or {}

        event_type_val = props.get("eventtype", "")
        event_id_val = props.get("eventid", "")
        episode_id_val = props.get("episodeid") or props.get("episode_id") or ""

        raw_is_current = props.get("iscurrent") if props.get("iscurrent") is not None else props.get("isCurrent")
        if raw_is_current is None:
            raw_is_current = props.get("current")

        is_current_bool = (raw_is_current is True) or (str(raw_is_current).strip().lower() in ("true", "1", "yes"))

        todate_dt = parse_gdacs_date(props.get("todate"))
        if not is_current_bool and todate_dt and todate_dt >= (now_utc - timedelta(hours=24)):
            is_current_bool = True

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

        cat_path = CATEGORY_PATH_MAP.get(event_type_val, "report.aspx")
        
        if episode_id_val:
            category_report_url = f"https://www.gdacs.org/{cat_path}?eventid={event_id_val}&episodeid={episode_id_val}&eventtype={event_type_val}"
            common_report_url = f"https://www.gdacs.org/report.aspx?eventid={event_id_val}&episodeid={episode_id_val}&eventtype={event_type_val}"
        else:
            category_report_url = f"https://www.gdacs.org/{cat_path}?eventid={event_id_val}&eventtype={event_type_val}"
            common_report_url = f"https://www.gdacs.org/report.aspx?eventid={event_id_val}&eventtype={event_type_val}"

        report_url = category_report_url

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
            "episodeid": episode_id_val,
            "activation_number": props.get("glide"),
            "event_type": event_type_val,
            "alert_level": props.get("alertlevel", "green"),
            "alert_score": props.get("alertscore", 0),
            "report_url": report_url,
            "category_report_url": category_report_url,
            "common_report_url": common_report_url,
            "last_updated": props.get("todate") or props.get("fromdate") or "",
            "severity": props.get("alertlevel", "green"),
            "is_current": is_current_bool,
        })

    print(f"\n⚙️  총 {len(results)}건 재난 추출 완료 (스킵: {skipped_no_geom}좌표누락, {skipped_duplicate}중복)")

    categorized = {}
    for r in results:
        etype = r.get("eventtype", "UNKNOWN")
        categorized.setdefault(etype, []).append(r)

    filtered_results = []
    for etype, items in categorized.items():
        def _get_date_key(x):
            dt = parse_gdacs_date(x.get("last_updated"))
            return dt or datetime.min.replace(tzinfo=timezone.utc)

        max_age_days = EVENT_TYPE_MAX_AGE_DAYS_BY_TYPE.get(etype, DEFAULT_EVENT_TYPE_MAX_AGE_DAYS)
        age_cutoff = now_utc - timedelta(days=max_age_days)

        valid_items = []
        for it in items:
            if it.get("is_current") is True:
                valid_items.append(it)
            elif _get_date_key(it) >= age_cutoff:
                valid_items.append(it)

        valid_items.sort(key=_get_date_key, reverse=True)
        filtered_results.extend(valid_items)

    final_counts = Counter(r.get("eventtype") for r in filtered_results)
    print(f"👉 최종 데이터 타입별 분포: {dict(final_counts)}")
    print(f"👉 총 수집 대상 목록 수: {len(filtered_results)}건")

    return filtered_results


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
    detail = fetch_json(detail_url, timeout=10)

    if not detail:
        return r, False

    props = detail.get("properties", detail) or {}

    if not r.get("episodeid"):
        ep_id = props.get("episodeid") or props.get("episode_id") or ""
        if ep_id:
            r["episodeid"] = ep_id
            cat_path = CATEGORY_PATH_MAP.get(eventtype, "report.aspx")
            r["category_report_url"] = f"https://www.gdacs.org/{cat_path}?eventid={eventid}&episodeid={ep_id}&eventtype={eventtype}"
            r["common_report_url"] = f"https://www.gdacs.org/report.aspx?eventid={eventid}&episodeid={ep_id}&eventtype={eventtype}"
            r["report_url"] = r["category_report_url"]

    tc_details = props.get("cyclonedetails") or {}
    eq_details = props.get("earthquakedetails") or {}
    fl_details = props.get("flooddetails") or {}
    vo_details = props.get("volcanodetails") or {}
    wf_details = props.get("wildfiredetails") or {}
    dr_details = props.get("droughtdetails") or {}

    r["exposed_population"] = (
        tc_details.get("rapidpop") or eq_details.get("rapidpop") or
        fl_details.get("rapidpop") or props.get("affectedpopulation")
    )
    r["exposed_population_description"] = (
        tc_details.get("rapidpopdescription") or eq_details.get("rapidpopdescription") or
        fl_details.get("rapidpopdescription")
    )
    r["vulnerability"] = props.get("vulnerabilitytext") or props.get("vulnerability")

    if eventtype == "TC":
        max_wind = tc_details.get("maxwindspeed")
        r["max_wind_speed_kmh"] = max_wind
        r["max_wind_speed_text"] = f"{max_wind} km/h" if max_wind else None
        r["max_storm_surge"] = tc_details.get("maxstormsurge")
        r["exposed_countries"] = tc_details.get("affectedcountries") or r.get("country")

    elif eventtype == "EQ":
        r["magnitude"] = eq_details.get("magnitude")
        r["depth_km"] = eq_details.get("depth")
        r["event_date_local"] = eq_details.get("episodedatelocal")

    elif eventtype == "FL":
        r["flood_area_sqkm"] = fl_details.get("area") or fl_details.get("floodedarea")
        r["severity_score"] = fl_details.get("severity")

    elif eventtype == "VO":
        r["vei"] = vo_details.get("vei")
        r["plume_height_m"] = vo_details.get("plumeheight")

    elif eventtype == "WF":
        r["burned_area_ha"] = wf_details.get("burnedarea") or wf_details.get("area")

    elif eventtype == "DR":
        r["drought_index"] = dr_details.get("droughtindex")

    resources = props.get("resources") or detail.get("resources") or []
    images_dict = props.get("images") or {}

    image_urls = []
    map_urls = []

    if isinstance(resources, list):
        for res in resources:
            res_url = res.get("url") if isinstance(res, dict) else str(res)
            if not res_url or not str(res_url).startswith(("http://", "https://")):
                continue

            res_url_clean = str(res_url).strip()
            if any(res_url_clean.lower().endswith(ext) for ext in [".png", ".jpg", ".jpeg", ".gif"]):
                image_urls.append(res_url_clean)
                if any(k in res_url_clean.lower() for k in ["map", "track", "wind", "overall", "current", "flood", "sat"]):
                    map_urls.append(res_url_clean)

    if isinstance(images_dict, dict):
        for key, val in images_dict.items():
            if isinstance(val, str) and val.strip().lower().startswith(("http://", "https://")):
                clean_v = val.strip()
                if clean_v not in image_urls:
                    image_urls.append(clean_v)

    r["image_urls"] = list(set(image_urls))
    r["map_urls"] = list(set(map_urls))
    r["overview_map_url"] = (
        images_dict.get("overviewmap") or images_dict.get("overviewmap_cached") or
        (map_urls[0] if map_urls else (image_urls[0] if image_urls else None))
    )

    sendai = props.get("sendai") or []
    severity = props.get("severitydata") or {}

    deaths, displaced, missing = 0, 0, 0
    deaths_found, displaced_found, missing_found = False, False, False
    sendai_details = []

    for s in sendai:
        name = (s.get("sendainame") or "").lower()
        try:
            val = int(re.sub(r"[^\d]", "", str(s.get("sendaivalue", "0"))) or 0)
        except ValueError:
            val = 0

        is_latest = str(s.get("latest", "")).strip().lower() in ("true", "1", "yes")

        if is_latest:
            if "death" in name:
                deaths += val; deaths_found = True
            elif "displaced" in name or "evacuat" in name:
                displaced += val; displaced_found = True
            elif "missing" in name:
                missing += val; missing_found = True

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

    detail_desc_candidates = [
        props.get("description"),
        props.get("htmldescription"),
        props.get("eventdescription"),
        props.get("longdescription"),
        detail.get("description") if isinstance(detail, dict) else None,
        detail.get("htmldescription") if isinstance(detail, dict) else None,
    ]
    detail_description = None
    for candidate in detail_desc_candidates:
        if not candidate:
            continue
        cleaned = re.sub(r'<[^>]*>', '', str(candidate)).strip()
        if cleaned and cleaned.lower() != str(r.get("title", "")).lower():
            detail_description = cleaned
            break
    r["detail_description"] = detail_description
    r["report_detail_url"] = detail_url

    return r, True


def enrich_disasters_parallel(results):
    results.sort(key=lambda r: SEVERITY_ORDER.get(str(r.get("alert_level", "green")).lower(), 3))

    print(f"\n🔄 [병렬 스레드 적용] 상세 정보(Enrich) 수집 시작... (총 {len(results)}개 대상, Workers: {MAX_WORKERS})")

    enriched_results = []
    enriched_ok = 0
    total_count = len(results)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_single_enrich, r): r for r in results}

        for idx, future in enumerate(as_completed(futures), 1):
            updated_record, success = future.result()
            enriched_results.append(updated_record)
            if success:
                enriched_ok += 1

            etype = updated_record.get("eventtype", "?")
            eid = updated_record.get("eventid", "?")
            status_text = "성공" if success else "실패"
            print(f"  ⚡ [{idx}/{total_count}] 완료: {etype}{eid} ({status_text})")

    print(f"\n🎉 상세정보 API 호출 {total_count}건 중 {enriched_ok}건 완벽 보강 완료!")
    return enriched_results


def load_sent_ids_history():
    file_existed = os.path.exists(SENT_IDS_FILEPATH)
    if not file_existed:
        return {}, False

    try:
        with open(SENT_IDS_FILEPATH, "r", encoding="utf-8") as f:
            raw = json.load(f)
        history = raw.get("history", {})
    except Exception as e:
        print(f"⚠️ 발송 이력 파일 파싱 스킵 (파일 깨짐 포착): {e}")
        return {}, False

    prune_cutoff = datetime.now(timezone.utc) - timedelta(days=PRUNE_AFTER_DAYS)
    pruned = {}
    for gdacs_id, entry in history.items():
        recorded_at = parse_gdacs_date(entry.get("recorded_at"))
        if recorded_at is not None and recorded_at < prune_cutoff:
            continue
        pruned[gdacs_id] = entry

    dropped = len(history) - len(pruned)
    print(f"🔍 발송 이력 파일 검사 완료: {len(pruned)}건 유지 ({dropped}건은 {PRUNE_AFTER_DAYS}일 초과로 정리)")
    return pruned, True


def save_sent_ids_history(history):
    tmp_path = SENT_IDS_FILEPATH + ".tmp"
    try:
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump({"history": history}, f, ensure_ascii=False, indent=2)
        os.replace(tmp_path, SENT_IDS_FILEPATH)
        print(f"💾 발송 이력 파일 갱신 완료: 총 {len(history)}건 기록")
    except Exception as e:
        print(f"⚠️ 발송 이력 파일 저장 실패: {e}")
        if os.path.exists(tmp_path):
            os.remove(tmp_path)


def main():
    results = fetch_disaster_list()

    if not results:
        print("\n🚨 [위험] 수집 및 필터링 완료된 데이터가 총 0건입니다.")
        print("💡 API 서버 장애 혹은 네트워크 타임아웃으로 예상되며, 기존 JSON 데이터를 보호하기 위해 프로그램 쓰기를 건너뜁니다.")
        sys.exit(0)

    results = enrich_disasters_parallel(results)

    temp_filepath = "data/realtime_disasters.json.tmp"
    final_filepath = "data/realtime_disasters.json"

    os.makedirs(os.path.dirname(final_filepath), exist_ok=True)

    sent_history, history_existed = load_sent_ids_history()

    is_fcm_ready = init_firebase()

    if is_fcm_ready:
        now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        if not history_existed:
            print("\n📢 [최초 실행 감지] 발송 이력 파일이 없어 현재 활성 재난을 이력에만 기록하고 푸시를 발송하지 않습니다.")
            for r in results:
                gdacs_id = r.get("gdacs_id")
                if gdacs_id:
                    sent_history[gdacs_id] = {
                        "last_updated": r.get("last_updated", ""),
                        "fingerprint": compute_disaster_fingerprint(r),
                        "recorded_at": now_iso
                    }
        else:
            print("\n📢 [정기 실행: 신규/갱신 재난 판별 및 푸시 알림 프로세스 가동]")
            new_disaster_count = 0

            for r in results:
                gdacs_id = r.get("gdacs_id")
                if not gdacs_id:
                    continue

                current_last_updated = r.get("last_updated", "")
                current_fingerprint = compute_disaster_fingerprint(r)
                prior_entry = sent_history.get(gdacs_id)

                is_new_or_updated = (prior_entry is None) or (prior_entry.get("fingerprint") != current_fingerprint)

                if is_new_or_updated:
                    new_disaster_count += 1

                    iso3_val = str(r.get("iso3", "")).upper().strip()
                    country_name = r.get("country", "Global")
                    iso2_backup = iso3_to_iso2(iso3_val)

                    reason = "신규" if prior_entry is None else "갱신"
                    print(f"  🆕 [{reason} 재난 포착] 제목: {r.get('title')} (ID: {gdacs_id})")

                    send_disaster_push(
                        country_iso2=iso2_backup,
                        country_iso3=iso3_val,
                        country_name=country_name,
                        disaster_title=r.get("title", "재난 경보"),
                        event_id=gdacs_id,
                        last_updated=current_last_updated
                    )

                    sent_history[gdacs_id] = {
                        "last_updated": current_last_updated,
                        "fingerprint": current_fingerprint,
                        "recorded_at": now_iso
                    }

                    save_sent_ids_history(sent_history)

            if new_disaster_count == 0:
                print("  - 지난 회차 대비 새롭게 발생하거나 갱신된 재난 정보가 없으므로 알림 발송 처리를 패스합니다.")
    else:
        print("\n⚠️ 파이어베이스 Secrets 환경변수가 없으므로 FCM 전송 엔진을 가동하지 않습니다.")

    save_sent_ids_history(sent_history)

    try:
        with open(temp_filepath, "w", encoding="utf-8") as f:
            json.dump({"status": "success", "data": results}, f, ensure_ascii=False, indent=2)

        os.replace(temp_filepath, final_filepath)
        print(f"\n💾 파일 원자적 저장 성공: '{final_filepath}' 업데이트 완료!")
    except Exception as e:
        print(f"\n❌ 파일 쓰기 오류 발생: {e}")
        if os.path.exists(temp_filepath):
            os.remove(temp_filepath)
        sys.exit(1)


if __name__ == "__main__":
    main()
