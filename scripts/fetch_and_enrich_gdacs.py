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
# 🌟 파이어베이스 모듈 주입
import firebase_admin
from firebase_admin import credentials, messaging

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

# 🌟 현재 진행 중인(활성) 재난만 표시하도록 변경
SHOW_ONLY_CURRENT = True

# ⭐️ [추가] ISO 3166-1 alpha-3 -> alpha-2 정식 매핑 테이블.
# 안드로이드 MyFirebaseMessagingService.java의 buildIso3ToIso2Map()과 동일한 매핑.
# 기존에는 iso3_val[:2] (앞 2글자 자르기)로 근사했는데, 예를 들어 MEX(멕시코)의
# 앞 2글자는 "ME"라서 실제로는 몬테네그로 코드와 충돌하는 등 오답이 잦았음.
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
    """정식 매핑 테이블 우선 조회. 매핑에 없는 값(희귀 지역코드 등)만 최후 수단으로 앞 2글자 사용."""
    iso3_val = (iso3_val or "").upper().strip()
    if iso3_val in ISO3_TO_ISO2:
        return ISO3_TO_ISO2[iso3_val]
    return iso3_val[:2] if len(iso3_val) == 3 else ""


# 🌟 파이어베이스 시스템 초기화 함수
def init_firebase():
    """깃허브 Secrets에 등록된 환경변수를 읽어 파이어베이스 관리를 시작합니다."""
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


# 🌟 [수정 완료] 실제 파이어베이스 'all' 토픽으로 데이터 페이로드 폭탄을 쏘는 함수
def send_disaster_push(country_iso2, country_iso3, country_name, disaster_title, event_id, last_updated):
    """
    모든 사용자가 수신할 수 있도록 'all' 토픽으로 푸시를 전송하되,
    안드로이드 앱 내부에서 즐겨찾기 필터에 무조건 걸리도록 풍부한 데이터 포맷을 구성하여 전송합니다.

    ⭐️ [버그 수정] android.notification(channel_id=...) 블록을 제거함.
    이 블록이 있으면 앱이 백그라운드/종료 상태일 때 FCM이 MyFirebaseMessagingService의
    onMessageReceived()를 거치지 않고 시스템이 직접 알림을 그리는데, title/body를
    notification 블록에 안 넣었기 때문에 시스템은 앱 이름만 표시하는 빈 알림을 띄웠음.
    (앱이 포그라운드일 때만 onMessageReceived가 호출되어 정상 작동했던 것)
    data-only 메시지로 보내면 백그라운드에서도 항상 onMessageReceived가 호출되어
    앱이 직접 국가명/시간을 조합한 알림을 만들 수 있음. channel_id는 앱 쪽
    postNotification()에서 NotificationCompat.Builder(context, CHANNEL_ID)로
    이미 지정하고 있으므로 여기서 중복 지정할 필요 없음.
    """
    topic_name = "all"
    
    # 안드로이드의 parseAsUtc()가 읽을 수 있도록 시간 값 포맷 안전 검증
    # 만약 기존 데이터에 시간 값이 누락되어 있다면 현재 UTC 시간으로 백업 적용
    if not last_updated:
        last_updated = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    elif not (last_updated.endswith('Z') or last_updated.endswith('z')):
        last_updated = str(last_updated).replace(" ", "T") + "Z"

    # 안드로이드가 수신 시 언어 번역에 참고할 수 있도록 국가명 정제
    c_name = country_name if country_name else "Global"

    try:
        message = messaging.Message(
            # ⭐️ [핵심 보완] 앱 내부의 즐겨찾기 파싱 로직(resolveEventIso2)의 모든 분기점을 원천 통과하도록 
            # 가능한 모든 형태의 국가 코드 및 텍스트 데이터 후보군을 변수 폭탄으로 주입
            data={
                "iso_code": str(country_iso2).upper(),  # 1순위 (예: MX)
                "isoCode": str(country_iso2).upper(),   # 2순위 (예: MX)
                "iso3": str(country_iso3).upper(),      # 3순위 (예: MEX) -> 내장 맵을 통해 ISO2로 역변환됨
                "country": c_name,                      # 4순위 텍스트 분기 대응 (예: Mexico)
                "countries": json.dumps([c_name, str(country_iso2).upper(), str(country_iso3).upper()]), # 배열 분기 대응
                
                # 시간 및 식별 데이터 연동 (알림 고유 키 발급 및 시간 파싱용)
                "last_updated": last_updated,
                "event_date": last_updated,
                "id": str(event_id) if event_id else "disaster_evt_999"
            },
            # ⭐️ [수정 완료] notification 블록 제거 -> data-only 메시지로 변경.
            # 백그라운드/종료 상태에서도 항상 onMessageReceived()가 호출되도록 함.
            android=messaging.AndroidConfig(
                priority="high"
            ),
            topic=topic_name
        )
        response = messaging.send(message)
        print(f"  👉 [FCM 알림 방송 성공] 토픽 채널: {topic_name} / 대상 국가: {c_name}({country_iso2}) (전송 ID: {response})")
    except Exception as e:
        print(f"  ❌ [FCM 알림 방송 실패] 토픽 채널: {topic_name} / 에러 내용: {e}")


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
        time.sleep(0.3)

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
        if SHOW_ONLY_CURRENT and is_current != "true":
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


def main():
    results = fetch_disaster_list()
    
    if not results:
        print("\n🚨 [위험] 수집 및 필터링 완료된 데이터가 총 0건입니다.")
        print("💡 API 서버 장애 혹은 네트워크 타임아웃으로 예상되며, 기존 JSON 데이터를 보호하기 위해 프로그램 쓰기를 건너뛰고 정상 안전 종료합니다.")
        sys.exit(0)

    results = enrich_disasters_parallel(results)

    temp_filepath = "data/realtime_disasters.json.tmp"
    final_filepath = "data/realtime_disasters.json"

    os.makedirs(os.path.dirname(final_filepath), exist_ok=True)

    existing_ids = set()
    if os.path.exists(final_filepath):
        try:
            with open(final_filepath, "r", encoding="utf-8") as f:
                old_json = json.load(f)
                old_data = old_json.get("data", [])
                existing_ids = {item["gdacs_id"] for item in old_data if "gdacs_id" in item}
            print(f"\n🔍 기존 JSON 파일 검사 완료: {len(existing_ids)}개의 고유 ID를 조회했습니다.")
        except Exception as e:
            print(f"\n⚠️ 기존 JSON 파싱 스킵 (최초 생성이거나 데이터 파일 깨짐 포착): {e}")

    # 🌟 파이어베이스 인증 가동
    is_fcm_ready = init_firebase()

    if is_fcm_ready:
        print("\n📢 [정기 실행: 신규 재난 판별 및 푸시 알림 프로세스 가동]")
        new_disaster_count = 0
        
        for r in results:
            gdacs_id = r.get("gdacs_id")
            
            # 수집된 최신 항목 중 기존 파일에 없던 새로운 gdacs_id를 가진 항목이 있을 때만 푸시 진입
            if gdacs_id and gdacs_id not in existing_ids:
                new_disaster_count += 1
                
                # ⭐️ [수정 완료] 기기 내부에서 ISO3 -> ISO2 역매핑을 지원하므로 
                # GDACS의 기본 세 가지 포맷(iso3, country, iso2 후보군생성)을 명확히 추출하여 배달
                iso3_val = str(r.get("iso3", "")).upper().strip()
                country_name = r.get("country", "Global")
                
                # ⭐️ [수정] 정식 ISO3->ISO2 매핑 테이블 사용 (기존의 앞 2글자 자르기 방식은
                # MEX->ME, KOR->KO처럼 오답이 많아서 제거함)
                iso2_backup = iso3_to_iso2(iso3_val)
                
                print(f"  🆕 [신규 재난 포착] 제목: {r.get('title')} (ID: {gdacs_id})")
                
                # 토픽 전체 방송(all) 및 기기단 완벽 매칭용 파라미터 전달
                send_disaster_push(
                    country_iso2=iso2_backup,
                    country_iso3=iso3_val,
                    country_name=country_name,
                    disaster_title=r.get("title", "재난 경보"),
                    event_id=gdacs_id,
                    last_updated=r.get("last_updated", "")
                )
        if new_disaster_count == 0:
            print("  - 지난 회차 대비 새롭게 발생한 재난 정보가 없으므로 알림 발송 처리를 안전하게 패스합니다.")
    else:
        print("\n⚠️ 파이어베이스 작동에 필요한 Secrets 값이 없으므로 신규 재난 비교 및 FCM 전송 엔진을 가동하지 않습니다.")

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
