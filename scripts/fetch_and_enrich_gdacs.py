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
# 🌟 파이어베이스 모듈 주입
import firebase_admin
from firebase_admin import credentials, messaging

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json",
}
DAYS_BACK = 30  # 최근 N일치 이벤트만 표시 (ENABLE_DATE_FILTER=True일 때만 적용)
ENABLE_DATE_FILTER = False  # False면 날짜 제한 없이 전부 가져옴 (디버깅/검증용)

# ⭐️ [수정] 개수 기반 top-N("MAX_ITEMS_PER_TYPE") 방식을 완전히 제거하고
# "최근성 기준" 필터로 교체. 기존 방식은 진행중인 재난(is_current=true)이라도
# 순위 경쟁에서 밀리면 화면/알림 목록에서 사라졌다가, last_updated가
# 살짝만 갱신돼도 다시 나타나는 churn(들쭉날쭉함)을 유발했음. 이는 지도 표시
# 불안정성뿐 아니라 "이미 보낸 재난을 신규로 재판정"하는 알림 중복 문제의
# 근본 원인이기도 했음.
# 새 방식: EVENT_TYPE_MAX_AGE_DAYS 이내에 갱신된 진행중 재난은 개수 상관없이 전부 유지.
EVENT_TYPE_MAX_AGE_DAYS = 7

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

# ⭐️ [신규] 발송 이력(sent_disaster_ids.json) 관련 설정.
# - 무제한 누적 리스트가 아니라 "gdacs_id -> 마지막으로 알림 보낸 시점의 last_updated"
#   형태로 저장. 같은 last_updated면 재발송하지 않고, last_updated가 실제로
#   바뀌면(=상황이 진짜 갱신됨) 정당한 재알림으로 간주해서 다시 보냄.
# - PRUNE_AFTER_DAYS보다 오래된 기록은 매 실행마다 자동으로 정리되어
#   파일이 무한정 커지지 않음(영구 보관 문제 없음).
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

# 🌟 현재 진행 중인(활성) 재난만 표시하도록 변경
SHOW_ONLY_CURRENT = True

# ⭐️ [추가] ISO 3166-1 alpha-3 -> alpha-2 정식 매핑 테이블.
# 안드로이드 MyFirebaseMessagingService.java의 buildIso3ToIso2Map()과 동일한 매핑.
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


def compute_disaster_fingerprint(r):
    """
    ⭐️ [신규] "실제로 내용이 바뀌었는지"를 판단하기 위한 지문(fingerprint).

    배경: GDACS는 is_current=true(진행중)인 재난의 last_updated(todate)를
    실질적인 내용 변화 없이도 "아직 진행중"이라는 의미로 계속 갱신한다.
    기존엔 last_updated 문자열이 조금이라도 바뀌면 무조건 재발송했기 때문에,
    이미 며칠 전부터 알려진 재난이 있는 국가를 막 즐겨찾기한 사용자가
    "지나간(이미 알고 있던) 재난"에 대한 알림을 받는 것처럼 느껴지는 문제가 있었다.

    last_updated는 지문에서 의도적으로 제외하고, 알림을 받을 가치가 있는
    "실제 상황 변화"만 담은 필드들로 지문을 구성한다. 이 지문이 이전과
    동일하면 last_updated만 갱신됐어도 재발송하지 않는다.
    """
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
    # ⚠️ 내장 hash()는 프로세스마다 무작위 솔트가 적용되는 해시 랜덤화 때문에
    # 같은 문자열이라도 매 GitHub Actions 실행(=매 프로세스)마다 다른 값이 나온다.
    # 반드시 hashlib처럼 결정론적인 해시를 써야 "실제로 안 바뀜"을 올바르게 판정할 수 있다.
    return hashlib.sha256(fingerprint_source.encode("utf-8")).hexdigest()


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


def send_disaster_push(country_iso2, country_iso3, country_name, disaster_title, event_id, last_updated):
    """
    모든 사용자가 수신할 수 있도록 'all' 토픽으로 푸시를 전송하되,
    안드로이드 앱 내부에서 즐겨찾기 필터에 무조건 걸리도록 풍부한 데이터 포맷을 구성하여 전송합니다.

    ⭐️ [버그 수정] android.notification(channel_id=...) 블록을 제거함.
    이 블록이 있으면 앱이 백그라운드/종료 상태일 때 FCM이 MyFirebaseMessagingService의
    onMessageReceived()를 거치지 않고 시스템이 직접 알림을 그리는데, title/body를
    notification 블록에 안 넣었기 때문에 시스템은 앱 이름만 표시하는 빈 알림을 띄웠음.
    data-only 메시지로 보내면 백그라운드에서도 항상 onMessageReceived가 호출되어
    앱이 직접 국가명/시간을 조합한 알림을 만들 수 있음.

    ⭐️ [신규] data에 "type": "travel_risk" 필드를 명시적으로 추가.
    클라이언트(MyFirebaseMessagingService)가 이 필드로 어떤 기능인지 라우팅하는
    구조로 바뀌었기 때문에, 필드가 없으면 하위호환을 위해 travel_risk로 간주하긴
    하지만 신규 발송분부터는 항상 명시적으로 보내는 것이 맞다(향후 다른 기능이
    추가되면 서버 쪽도 그 기능에 맞는 type을 명시적으로 채워 보내야 함).

    ⭐️ [신규] data에 "alarm_id" 필드 추가. gdacs_id(재난 자체의 ID)와는 별개로,
    "이 발송(알람) 자체"를 식별하는 고유 ID다. 같은 재난이라도 여러 번 갱신되면서
    push가 여러 번 나갈 수 있는데, 각 발송 건을 개별적으로 추적/디버깅하고 싶을 때
    (예: Logcat에서 특정 알림이 언제 어떤 값으로 발송됐는지 추적) 이 값으로 구분한다.
    """
    topic_name = "all"

    if not last_updated:
        last_updated = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    elif not (last_updated.endswith('Z') or last_updated.endswith('z')):
        last_updated = str(last_updated).replace(" ", "T") + "Z"

    c_name = country_name if country_name else "Global"
    alarm_id = uuid.uuid4().hex  # ⭐️ [신규] 이 발송 건 고유 식별자

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

    # ⭐️ [수정] 개수 기반 top-N을 완전히 제거하고 "최근성 기준" 필터로 교체.
    # 진행중(is_current=true)인 재난은 EVENT_TYPE_MAX_AGE_DAYS 이내에 갱신됐다면
    # 개수 상관없이 전부 유지한다. 인위적인 상한을 두지 않음으로써
    # "순위 경쟁으로 밀려났다가 다시 나타나는" churn을 원천 차단한다.
    print(f"\n✂️  각 타입별 최근 {EVENT_TYPE_MAX_AGE_DAYS}일 이내 갱신된 진행중 재난만 유지 (개수 제한 없음)")
    categorized = {etype: [] for etype in EVENT_TYPES}

    for r in results:
        etype = r.get("eventtype")
        if etype in categorized:
            categorized[etype].append(r)

    age_cutoff = datetime.now(timezone.utc) - timedelta(days=EVENT_TYPE_MAX_AGE_DAYS)
    filtered_results = []

    for etype, items in categorized.items():
        def _get_date_key(x):
            dt = parse_gdacs_date(x.get("last_updated"))
            return dt or datetime.min.replace(tzinfo=timezone.utc)

        recent_items = [it for it in items if _get_date_key(it) >= age_cutoff]
        recent_items.sort(key=_get_date_key, reverse=True)

        filtered_results.extend(recent_items)
        dropped = len(items) - len(recent_items)
        print(f"  • [{etype}] 총 {len(items)}건 중 최근 {len(recent_items)}건 유지 ({dropped}건은 {EVENT_TYPE_MAX_AGE_DAYS}일 초과로 제외)")

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

        # ⭐️ [버그 수정] GDACS API는 sendai 항목마다 "latest" 플래그를 내려준다 -
        # 같은 지역/항목에 대해 시간이 지나며 수치가 갱신되면, 과거 값은 latest=false로
        # 남아있고 가장 최근 값만 latest=true가 된다. 그런데 기존 코드는 이 플래그를
        # sendai_details에 캡처만 해두고 실제 합산(deaths/displaced/missing) 로직에서는
        # 전혀 쓰지 않아서, "이미 무효화된 과거 수치"까지 전부 더해버리는 문제가 있었다.
        # 그 결과 GDACS 공식 리포트 페이지의 "현재 유효한 총계"와 앱에 표시되는 숫자가
        # 서로 달라졌다(예: 사망자 13명 vs 공식 20명). latest=true인 항목만 합산한다.
        is_latest = str(s.get("latest", "")).strip().lower() in ("true", "1", "yes")

        if is_latest:
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


def load_sent_ids_history():
    """
    ⭐️ [신규] 발송 이력 로드. {gdacs_id: {last_updated, recorded_at}} 형태.
    PRUNE_AFTER_DAYS보다 오래된 항목은 이 시점에 걸러내서 반환하므로
    파일이 무한정 누적되지 않는다.

    ⭐️ [부트스트랩 보정] 반환값에 더해 "이 파일이 원래 존재했는지" 여부도
    함께 반환한다. 파일이 아예 없던 최초 실행(새 이력 시스템을 막 도입한 직후)에는
    현재 활성 재난 전부가 "신규"로 오판되어 즐겨찾기 국가에 한꺼번에 알림이
    몰리는 문제가 있었음. 파일이 원래 없었다면 이번 실행은 발송 없이 이력만
    조용히 채워서(seed), 다음 실행부터 정상적으로 증분 비교되게 한다.
    """
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
            continue  # 오래된 기록은 자동 정리
        pruned[gdacs_id] = entry

    dropped = len(history) - len(pruned)
    print(f"🔍 발송 이력 파일 검사 완료: {len(pruned)}건 유지 ({dropped}건은 {PRUNE_AFTER_DAYS}일 초과로 정리)")
    return pruned, True


def save_sent_ids_history(history):
    """⭐️ [신규] 발송 이력 저장 (원자적 쓰기)."""
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
        print("💡 API 서버 장애 혹은 네트워크 타임아웃으로 예상되며, 기존 JSON 데이터를 보호하기 위해 프로그램 쓰기를 건너뛰고 정상 안전 종료합니다.")
        sys.exit(0)

    results = enrich_disasters_parallel(results)

    temp_filepath = "data/realtime_disasters.json.tmp"
    final_filepath = "data/realtime_disasters.json"

    os.makedirs(os.path.dirname(final_filepath), exist_ok=True)

    # ⭐️ [수정] "이미 보냈는지" 판단 기준을 표시용 파일(realtime_disasters.json)이 아니라
    # 별도의 발송 이력 파일(sent_disaster_ids.json)로 완전히 분리.
    sent_history, history_existed = load_sent_ids_history()

    # 🌟 파이어베이스 인증 가동
    is_fcm_ready = init_firebase()

    if is_fcm_ready:
        now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        if not history_existed:
            # ⭐️ [부트스트랩] 이력 파일이 처음 생기는 실행 -> 현재 활성 재난 전부를
            # "이미 처리됨"으로 조용히 기록만 하고 푸시는 보내지 않음.
            # (없으면 기존 활성 재난 20~30건이 전부 "신규"로 오판되어
            #  즐겨찾기 국가에 한꺼번에 알림이 몰리는 문제가 있었음)
            print("\n📢 [최초 실행 감지] 발송 이력 파일이 없어 현재 활성 재난을 이력에만 기록하고, 이번 회차는 푸시를 발송하지 않습니다.")
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

                # ⭐️ [수정] last_updated(GDACS todate) 대신 fingerprint(실제 내용) 비교로 변경.
                # GDACS는 진행중(is_current=true) 재난의 todate를 내용 변화 없이도 계속
                # 갱신하므로, last_updated만 보고 판단하면 "이미 알려진 재난"이 계속
                # 신규/갱신으로 오판되어 재발송된다 — 특히 막 즐겨찾기한 사용자에게는
                # "지나간 재난"에 대한 알림처럼 느껴지는 원인이었다.
                # fingerprint는 심각도/사망자/변위/실종/제목/요약처럼 알림을 받을
                # 가치가 있는 실제 변화만 담으므로, todate만 흘러가는 경우는 걸러진다.
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

                    # ⭐️ [신규] push를 보낸 "직후" 즉시 이력을 디스크에 저장.
                    # 기존엔 루프가 다 끝난 뒤(save_sent_ids_history(sent_history)) 한 번에만
                    # 저장했는데, 이제 워크플로가 cancel-in-progress: true라 실행이
                    # 중간에 취소될 수 있다. 만약 push는 이미 나갔는데 이력 저장 전에
                    # 취소되면, 다음 실행이 같은 이벤트를 "또 신규"로 오판해서
                    # 중복 알림을 보내게 된다. push마다 즉시 저장하면 이 위험이 없다.
                    save_sent_ids_history(sent_history)

            if new_disaster_count == 0:
                print("  - 지난 회차 대비 새롭게 발생하거나 갱신된 재난 정보가 없으므로 알림 발송 처리를 안전하게 패스합니다.")
    else:
        print("\n⚠️ 파이어베이스 작동에 필요한 Secrets 값이 없으므로 신규 재난 비교 및 FCM 전송 엔진을 가동하지 않습니다.")

    # ⭐️ 루프 중간중간 이미 저장됐더라도, 부트스트랩 시딩 등 다른 갱신분까지
    # 포함해 마지막에 한 번 더 전체 상태를 저장(멱등 연산이라 안전함).
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
