import json
import re
import time
import hashlib
import uuid
import urllib.request
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
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.gdacs.org/",
}

# geteventdata 상세조회 재시도 설정 (403/429/5xx에 대해서만 재시도)
DETAIL_RETRY_COUNT = 3
DETAIL_RETRY_DELAY_SEC = 3.0

# 🔧 GDACS 공식 홈페이지(gdacs.org/default.aspx)는
#    "Map of disaster alerts in the past 4 days" 라고 명시하고 있음.
#    즉 지도에 뿌려지는 마커는 최근 4일 이내 이벤트로 한정됨.
#    단, 아래 두 타입은 날짜 기준이 아니라 별도 규칙을 따름:
#    - 가뭄(DR): 날짜 무관, 진행 중인 이벤트는 전부 표시
#    - 산불(WF): 진행 중인 Orange/Red는 날짜 무관 전부 표시.
#                Green은 "소실면적 10,000ha 초과"일 때만 표시
#      (인구 조건은 GDACS API로 노출되지 않아 면적 기준만 적용)
#      (출처: https://www.gdacs.org/knowledge/models_wf.aspx)
DAYS_BACK = 4
ENABLE_DATE_FILTER = True

# 순수 날짜 필터를 적용하지 않을 이벤트 타입 (별도 규칙으로 처리)
DATE_FILTER_EXEMPT_TYPES = {"DR", "WF"}

# 산불(WF) 전용 기준 (GDACS 홈페이지 정책)
WF_GREEN_AREA_THRESHOLD_HA = 10000

# 상세조회(geteventdata)와 이미지 검증은 gdacs.org에 동시에 몰리면
# WAF의 "짧은 시간 내 동시 연결 수" 탐지에 걸려 403을 유발할 수 있어
# 훨씬 보수적인 동시성을 별도로 사용한다.
ENRICH_WORKERS = 3         # geteventdata 동시 호출 수 (outer)
IMAGE_VALIDATE_WORKERS = 2 # 이미지 URL 검증 동시 호출 수 (inner, enrich 스레드마다)
ENRICH_STAGGER_SEC = 0.3   # 스레드 제출 사이 최소 간격(지터)

EVENT_TYPES = ["EQ", "TC", "FL", "VO", "WF", "DR", "TS"]

BASE_URL_TEMPLATE = (
    "https://www.gdacs.org/gdacsapi/api/events/geteventlist/SEARCH"
    "?eventlist={eventtype}&alertlevel=green%3Borange%3Bred"
)

PAGE_RETRY_COUNT = 3        
PAGE_RETRY_DELAY_SEC = 2.0  

SENT_IDS_FILEPATH = "data/sent_disaster_ids.json"
PRUNE_AFTER_DAYS = 45

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
SHOW_ONLY_CURRENT = True

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


def sanitize_severity_text(raw_text):
    if not raw_text:
        return raw_text
    replacements = [
        (r"\bgreen\b", "low"),
        (r"\borange\b", "moderate"),
        (r"\bred\b", "high"),
    ]
    result = str(raw_text)
    for pattern, replacement in replacements:
        result = re.sub(pattern, replacement, result, flags=re.IGNORECASE)
    return result


def parse_gdacs_datetime(date_str):
    """GDACS API가 내려주는 다양한 날짜 포맷을 안전하게 파싱해서
    timezone-aware(UTC) datetime으로 반환. 실패 시 None."""
    if not date_str or not isinstance(date_str, str):
        return None

    s = date_str.strip()
    if not s:
        return None

    # 'Z' 접미사는 fromisoformat이 파이썬 버전에 따라 못 읽을 수 있어 치환
    if s.endswith("Z") or s.endswith("z"):
        s = s[:-1] + "+00:00"

    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        # 밀리초 등 fromisoformat이 못 읽는 케이스에 대한 폴백
        for fmt in ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
            try:
                dt = datetime.strptime(s, fmt)
                break
            except ValueError:
                dt = None
        if dt is None:
            return None

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    return dt


def is_within_recent_window(event_type_val, fromdate_str, todate_str, days_back, now_utc):
    """GDACS 홈페이지 지도 로직 재현:
    - 가뭄(DR), 산불(WF)은 별도 규칙으로 처리하므로 여기선 건너뜀
    - 그 외 타입은 최근 days_back일 이내 이벤트만 포함
      (todate 우선, 없으면 fromdate 사용)
    """
    if event_type_val in DATE_FILTER_EXEMPT_TYPES:
        return True

    ref_dt = parse_gdacs_datetime(todate_str) or parse_gdacs_datetime(fromdate_str)
    if ref_dt is None:
        # 날짜를 못 읽으면 안전하게 제외 (오래된 잔재 데이터가 새 마커처럼 남는 것 방지)
        return False

    cutoff = now_utc - timedelta(days=days_back)
    return ref_dt >= cutoff


def is_url_accessible(url, timeout=3.5):
    if not url or not isinstance(url, str):
        return False
    if "{" in url or "}" in url:
        return False
    try:
        req = urllib.request.Request(url, headers=HEADERS, method="HEAD")
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status == 200
    except Exception:
        return False


def validate_image_urls_parallel(url_list):
    if not url_list:
        return []
    
    unique_urls = list(dict.fromkeys(url_list))
    valid_urls = []
    
    with ThreadPoolExecutor(max_workers=IMAGE_VALIDATE_WORKERS) as executor:
        future_to_url = {executor.submit(is_url_accessible, url): url for url in unique_urls}
        for future in as_completed(future_to_url):
            url = future_to_url[future]
            try:
                if future.result():
                    valid_urls.append(url)
            except Exception:
                pass
    return valid_urls


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
    ]
    fingerprint_source = "|".join(parts)
    return hashlib.sha256(fingerprint_source.encode("utf-8")).hexdigest()


def init_firebase():
    if not firebase_admin._apps:
        cred_json_str = os.environ.get('FIREBASE_SERVICE_ACCOUNT_JSON')
        if not cred_json_str:
            print("⚠️ [FCM] FIREBASE_SERVICE_ACCOUNT_JSON 환경변수가 없어 알림을 건너뜁니다.")
            return False
        try:
            cred_json = json.loads(cred_json_str)
            cred = credentials.Certificate(cred_json)
            firebase_admin.initialize_app(cred)
            return True
        except Exception as e:
            print(f"❌ [FCM] 파이어베이스 초기화 실패: {e}")
            return False
    return True


def send_disaster_push(country_iso2, country_iso3, country_name, disaster_title, alert_level, event_id, last_updated):
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
                "disaster_title": str(disaster_title),
                "alert_level": str(alert_level).lower(),
                "last_updated": last_updated,
                "id": str(event_id) if event_id else "disaster_evt_999"
            },
            android=messaging.AndroidConfig(priority="high"),
            topic=topic_name
        )
        response = messaging.send(message)
        print(f"  👉 [FCM 알림 전송] [{alert_level.upper()}] {c_name}({country_iso2}) - {disaster_title} (ID: {response})")
    except Exception as e:
        print(f"  ❌ [FCM 전송 실패] {e}")


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


def fetch_json(url, timeout=8, retries=DETAIL_RETRY_COUNT, delay=DETAIL_RETRY_DELAY_SEC):
    last_err_msg = None

    for attempt in range(1, retries + 1):
        try:
            req = urllib.request.Request(url, headers=HEADERS)
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            try:
                body_preview = e.read().decode("utf-8", errors="replace")[:300]
            except Exception:
                body_preview = "(본문 읽기 실패)"
            last_err_msg = f"{e} / 응답 헤더: {dict(e.headers)} / 본문 일부: {body_preview}"

            # 403/429/5xx는 일시적 차단·과부하일 수 있으니 재시도, 그 외(404 등)는 즉시 포기
            if e.code not in (403, 429, 500, 502, 503, 504):
                break
        except Exception as e:
            last_err_msg = str(e)

        if attempt < retries:
            time.sleep(delay * attempt)  # 점증 대기 (3s, 6s, ...)

    print(f"  ⚠️ 상세정보 조회 실패: {url} ({last_err_msg})")
    return None


def fetch_page_with_retry(url, retries=PAGE_RETRY_COUNT, delay=PAGE_RETRY_DELAY_SEC):
    status, body = None, None
    for attempt in range(1, retries + 1):
        try:
            req = urllib.request.Request(url, headers=HEADERS)
            with urllib.request.urlopen(req, timeout=15) as resp:
                status = resp.status
                body = resp.read()
        except Exception:
            status, body = None, None

        if status == 200 and body:
            return status, body

        if attempt < retries:
            time.sleep(delay)

    return status, body


def fetch_events_for_type(event_type):
    base_url = BASE_URL_TEMPLATE.format(eventtype=event_type)
    all_features = []
    seen_keys = set()
    page = 1

    while True:
        url = f"{base_url}&pagenumber={page}"
        status, body = fetch_page_with_retry(url)

        if status != 200 or not body:
            break

        try:
            page_json = json.loads(body)
        except Exception:
            break

        page_features = page_json.get("features")
        if not page_features:
            break

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

        page += 1

    return all_features


def fetch_disaster_list():
    all_features = []

    print("==================================================")
    print("🚀 실시간 재난 데이터 수집 가동 (GDACS API)")
    print("==================================================")

    for idx, event_type in enumerate(EVENT_TYPES, 1):
        type_features = fetch_events_for_type(event_type)
        print(f"📡 [{idx}/{len(EVENT_TYPES)}] GDACS [{event_type}] 수집 완료 ({len(type_features)}건)")
        all_features.extend(type_features)
        time.sleep(0.2)

    results = []
    seen_result_keys = set()
    now_utc = datetime.now(timezone.utc)

    skipped_old = 0
    skipped_not_current = 0
    skipped_wf_small = 0

    for feat in all_features:
        props = feat.get("properties", {}) or {}
        geom = feat.get("geometry", {}) or {}

        event_type_val = props.get("eventtype", "")
        event_id_val = props.get("eventid", "")

        is_current = str(props.get("iscurrent", "")).strip().lower()
        if SHOW_ONLY_CURRENT and is_current != "true":
            skipped_not_current += 1
            continue

        if event_type_val == "WF":
            # 🔧 산불 전용 규칙 (GDACS 홈페이지 정책):
            #    - Orange/Red: 진행 중이면 날짜 무관 전부 포함
            #    - Green: 소실면적 10,000ha 초과일 때만 포함
            #      (인구 조건은 GDACS API로 노출되지 않아 적용하지 않음)
            alertlevel_lower = str(props.get("alertlevel", "")).strip().lower()
            if alertlevel_lower not in ("orange", "red"):
                try:
                    area_ha = float(props.get("severitydata", {}).get("severity", 0) or 0)
                except (TypeError, ValueError):
                    area_ha = 0.0

                if area_ha <= WF_GREEN_AREA_THRESHOLD_HA:
                    skipped_wf_small += 1
                    continue
        elif ENABLE_DATE_FILTER:
            # 🔧 GDACS 홈페이지 지도와 동일하게 "최근 N일" 이내 이벤트만 표시
            #    (가뭄은 예외적으로 진행 중이면 항상 포함)
            if not is_within_recent_window(
                event_type_val,
                props.get("fromdate"),
                props.get("todate"),
                DAYS_BACK,
                now_utc,
            ):
                skipped_old += 1
                continue

        if event_type_val and event_id_val:
            result_key = f"{event_type_val}{event_id_val}"
            if result_key in seen_result_keys:
                continue
            seen_result_keys.add(result_key)

        centroid = get_centroid(geom)
        if centroid is None:
            continue
        lng, lat = centroid

        if not (-180 <= lng <= 180 and -90 <= lat <= 90):
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

        report_url = f"https://www.gdacs.org/report.aspx?eventtype={event_type_val}&eventid={event_id_val}"

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
            "glide": props.get("glide"),
            "alert_level": props.get("alertlevel", "green"),
            "alert_score": props.get("alertscore", 0),
            "report_url": report_url,
            "last_updated": props.get("todate") or props.get("fromdate") or "",
            "is_current": is_current == "true",
        })

    print(
        f"🧹 필터링: iscurrent 아님 {skipped_not_current}건 / "
        f"{DAYS_BACK}일 초과(DR·WF 제외) {skipped_old}건 / "
        f"Green 산불 면적 미달 {skipped_wf_small}건 "
        f"제외 → 최종 {len(results)}건"
    )

    return results


def process_single_enrich(r):
    r_eventtype = r.get("eventtype", "")
    r_alertlevel = str(r.get("alert_level", "green")).lower()
    
    r["report_description"] = (
        f"This {EVENT_TYPE_NAME.get(r_eventtype, 'event')} could have a "
        f"{IMPACT_LEVEL.get(r_alertlevel, 'unknown')} impact on affected communities, "
        f"based on {IMPACT_BASIS.get(r_eventtype, 'the severity')} and the "
        f"exposure and vulnerability of the population nearby."
    )

    eventtype = r.get("eventtype")
    eventid = r.get("eventid")

    if not eventtype or not eventid:
        return r, True

    detail_url = f"https://www.gdacs.org/gdacsapi/api/events/geteventdata?eventtype={eventtype}&eventid={eventid}"
    detail = fetch_json(detail_url)

    if not detail:
        return r, True

    props = detail.get("properties", detail) or {}
    sendai = props.get("sendai") or []
    severity = props.get("severitydata") or {}
    images = props.get("images") or {}
    eq_details = props.get("earthquakedetails") or {}

    r["affected_countries"] = props.get("affectedcountries") or []

    deaths, displaced, missing = None, None, None
    sendai_details = []

    for s in sendai:
        name = (s.get("sendainame") or "").lower()
        raw_value = s.get("sendaivalue", "0")

        try:
            val = int(re.sub(r"[^\d]", "", str(raw_value)) or 0)
        except ValueError:
            val = 0

        is_latest_raw = s.get("latest", "")
        is_latest = str(is_latest_raw).strip().lower() in ("true", "1", "yes")

        if is_latest:
            if "death" in name:
                deaths = (deaths or 0) + val
            elif "displaced" in name or "evacuat" in name:
                displaced = (displaced or 0) + val
            elif "missing" in name:
                missing = (missing or 0) + val

        sendai_details.append({
            "type": s.get("sendaitype"),
            "name": s.get("sendainame"),
            "value": s.get("sendaivalue"),
            "description": s.get("description"),
            "date": s.get("dateinsert"),
        })

    r["deaths"] = deaths
    r["displaced"] = displaced
    r["missing"] = missing
    r["severity_text"] = sanitize_severity_text(severity.get("severitytext"))
    r["impact_history"] = sendai_details
    r["impact_description"] = (sendai_details[-1]["description"][:300] if sendai_details else None)

    # 이미지 URL 추출 및 404 실시간 검증
    raw_image_candidates = []
    if isinstance(images, dict):
        for key, val in images.items():
            if isinstance(val, str) and val.strip().lower().startswith(("http://", "https://")):
                if "{" not in val and "}" not in val:
                    raw_image_candidates.append(val.strip())

    valid_image_urls = validate_image_urls_parallel(raw_image_candidates)

    alert_cap = r_alertlevel.capitalize()
    fallback_icon = f"https://www.gdacs.org/images/gdacs_icons/big/{alert_cap}/{r_eventtype}.png"

    r["image_urls"] = valid_image_urls

    if valid_image_urls:
        r["overview_map_url"] = valid_image_urls[0]
    else:
        r["overview_map_url"] = fallback_icon

    r["magnitude"] = eq_details.get("magnitude")
    r["depth_km"] = eq_details.get("depth")
    r["exposed_population"] = eq_details.get("rapidpop")

    return r, True


def enrich_disasters_parallel(results):
    results.sort(key=lambda r: SEVERITY_ORDER.get(str(r.get("alert_level", "green")).lower(), 3))

    print(f"\n🔄 공식 리포트 상세 및 이미지 유효성(404) 검증 중... ({len(results)}건 대상)")

    enriched_results = []
    with ThreadPoolExecutor(max_workers=ENRICH_WORKERS) as executor:
        futures = {}
        for r in results:
            futures[executor.submit(process_single_enrich, r)] = r
            time.sleep(ENRICH_STAGGER_SEC)  # gdacs.org에 몰리는 동시 연결을 완화
        for future in as_completed(futures):
            updated_record, _ = future.result()
            enriched_results.append(updated_record)

    print("🎉 상세 데이터 및 이미지 유효성 검증 완료!")
    return enriched_results


def load_sent_ids_history():
    if not os.path.exists(SENT_IDS_FILEPATH):
        return {}, False
    try:
        with open(SENT_IDS_FILEPATH, "r", encoding="utf-8") as f:
            raw = json.load(f)
        return raw.get("history", {}), True
    except Exception:
        return {}, False


def save_sent_ids_history(history):
    tmp_path = SENT_IDS_FILEPATH + ".tmp"
    try:
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump({"history": history}, f, ensure_ascii=False, indent=2)
        os.replace(tmp_path, SENT_IDS_FILEPATH)
    except Exception:
        if os.path.exists(tmp_path): os.remove(tmp_path)


def main():
    results = fetch_disaster_list()

    if not results:
        print("🚨 수집된 데이터가 없습니다. 응답 실패 방지를 위해 기존 데이터를 보존합니다.")
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
            for r in results:
                gdacs_id = r.get("gdacs_id")
                if gdacs_id:
                    sent_history[gdacs_id] = {
                        "last_updated": r.get("last_updated", ""),
                        "fingerprint": compute_disaster_fingerprint(r),
                        "recorded_at": now_iso
                    }
        else:
            for r in results:
                gdacs_id = r.get("gdacs_id")
                if not gdacs_id: continue

                current_fingerprint = compute_disaster_fingerprint(r)
                prior_entry = sent_history.get(gdacs_id)

                if (prior_entry is None) or (prior_entry.get("fingerprint") != current_fingerprint):
                    iso3_val = str(r.get("iso3", "")).upper().strip()

                    send_disaster_push(
                        country_iso2=iso3_to_iso2(iso3_val),
                        country_iso3=iso3_val,
                        country_name=r.get("country", "Global"),
                        disaster_title=r.get("title", "Disaster Alert"),
                        alert_level=r.get("alert_level", "green"),
                        event_id=gdacs_id,
                        last_updated=r.get("last_updated", "")
                    )

                    sent_history[gdacs_id] = {
                        "last_updated": r.get("last_updated", ""),
                        "fingerprint": current_fingerprint,
                        "recorded_at": now_iso
                    }

    save_sent_ids_history(sent_history)

    try:
        with open(temp_filepath, "w", encoding="utf-8") as f:
            json.dump({"status": "success", "data": results}, f, ensure_ascii=False, indent=2)

        os.replace(temp_filepath, final_filepath)
        print(f"\n💾 필터링 적용 최종 데이터 저장 완료: '{final_filepath}'")
    except Exception as e:
        print(f"❌ 저장 오류: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
