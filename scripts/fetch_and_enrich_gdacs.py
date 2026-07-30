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
DAYS_BACK = 30  # 최근 N일치 이벤트만 표시 (ENABLE_DATE_FILTER=True일 때만 적용)[span_1](start_span)[span_1](end_span)
ENABLE_DATE_FILTER = False  # False면 날짜 제한 없이 전부 가져옴 (디버깅/검증용)[span_2](start_span)[span_2](end_span)

EVENT_TYPE_MAX_AGE_DAYS_BY_TYPE = {
    "EQ": 3,    # 지진 - 본진 이후 여진 정도만 짧게 추적[span_3](start_span)[span_3](end_span)
    "WF": 2,    # 산불 - 위성 열 감지가 끊기면 사실상 진화된 것으로 봄[span_4](start_span)[span_4](end_span)
    "TS": 2,    # 쓰나미 - 발생 후 파고 관측이 끝나면 빠르게 종료[span_5](start_span)[span_5](end_span)
    "TC": 5,    # 태풍/사이클론 - 소멸까지 며칠 정도 걸림[span_6](start_span)[span_6](end_span)
    "FL": 7,    # 홍수 - 배수/복구까지 시간이 걸려 기존 기준 유지[span_7](start_span)[span_7](end_span)
    "VO": 10,   # 화산 - 분화 활동이 길게 이어질 수 있음[span_8](start_span)[span_8](end_span)
    "DR": 14,   # 가뭄 - 원래 변화가 느린 재난이라 가장 길게 유지[span_9](start_span)[span_9](end_span)
}
DEFAULT_EVENT_TYPE_MAX_AGE_DAYS = 7  # 매핑에 없는 타입 대비 기본값[span_10](start_span)[span_10](end_span)

MAX_WORKERS = 8[span_11](start_span)[span_11](end_span)
EVENT_TYPES = ["EQ", "TC", "FL", "VO", "WF", "DR", "TS"][span_12](start_span)[span_12](end_span)

# ⭐️ [개선] GDACS SEARCH API는 날짜 범위(fromdate, todate) 파라미터가 없으면 결과를 빈 값으로 반환하는 경향이 있음
# 요청 시점 기준 최근 30일간의 날짜 범위를 동적으로 계산하여 API URL을 생성하도록 함수화
def get_base_url(event_type):
    now = datetime.now(timezone.utc)
    from_date = (now - timedelta(days=30)).strftime("%Y-%m-%d")
    to_date = now.strftime("%Y-%m-%d")
    return (
        f"https://www.gdacs.org/gdacsapi/api/events/geteventlist/SEARCH"
        f"?eventlist={event_type}&fromdate={from_date}&todate={to_date}&alertlevel=green%3Borange%3Bred"
    )

PAGE_RETRY_COUNT = 3[span_13](start_span)[span_13](end_span)
PAGE_RETRY_DELAY_SEC = 2.0[span_14](start_span)[span_14](end_span)

SENT_IDS_FILEPATH = "data/sent_disaster_ids.json[span_15](start_span)"[span_15](end_span)
PRUNE_AFTER_DAYS = 45[span_16](start_span)[span_16](end_span)

GDACS_COUNTRY_MAP = {
    "republic of korea": "South Korea",
    "korea, republic of": "South Korea",
    "united states of america": "United States",
    "us": "United States",
    "viet nam": "Vietnam",
    "russian federation": "Russia",
    "syrian arab republic": "Syria",
    "turkiye": "Turkey"
}[span_17](start_span)[span_17](end_span)

EVENT_TYPE_NAME = {
    "EQ": "earthquake", "TC": "tropical cyclone", "FL": "flood",
    "WF": "wildfire", "VO": "volcanic event", "DR": "drought",
    "TS": "tsunami",
}[span_18](start_span)[span_18](end_span)
IMPACT_LEVEL = {"green": "low", "orange": "medium", "red": "significant"}[span_19](start_span)[span_19](end_span)
IMPACT_BASIS = {
    "EQ": "the magnitude", "TC": "the wind speed", "FL": "the flood extent",
    "WF": "the affected area", "VO": "the eruption size", "DR": "the drought severity",
    "TS": "the wave height",
}[span_20](start_span)[span_20](end_span)

SEVERITY_ORDER = {"red": 0, "orange": 1, "green": 2}[span_21](start_span)[span_21](end_span)

SHOW_ONLY_CURRENT = True[span_22](start_span)[span_22](end_span)

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
}[span_23](start_span)[span_23](end_span)


def iso3_to_iso2(iso3_val: str) -> str:
    iso3_val = (iso3_val or "").upper().strip()[span_24](start_span)[span_24](end_span)
    if iso3_val in ISO3_TO_ISO2:
        return ISO3_TO_ISO2[iso3_val][span_25](start_span)[span_25](end_span)
    return iso3_val[:2] if len(iso3_val) == 3 else "[span_26](start_span)"[span_26](end_span)


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
    ][span_27](start_span)[span_27](end_span)
    fingerprint_source = "|".join(parts)[span_28](start_span)[span_28](end_span)
    return hashlib.sha256(fingerprint_source.encode("utf-8")).hexdigest()[span_29](start_span)[span_29](end_span)


def init_firebase():
    if not firebase_admin._apps:
        cred_json_str = os.environ.get('FIREBASE_SERVICE_ACCOUNT_JSON')[span_30](start_span)[span_30](end_span)
        if not cred_json_str:
            print("⚠️  [FCM] FIREBASE_SERVICE_ACCOUNT_JSON 환경변수가 없어서 알림 발송을 건너뜁니다.")[span_31](start_span)[span_31](end_span)
            return False
        try:
            cred_json = json.loads(cred_json_str)[span_32](start_span)[span_32](end_span)
            cred = credentials.Certificate(cred_json)[span_33](start_span)[span_33](end_span)
            firebase_admin.initialize_app(cred)[span_34](start_span)[span_34](end_span)
            return True
        except Exception as e:
            print(f"❌ [FCM] 파이어베이스 라이브러리 초기화 중 오류: {e}")[span_35](start_span)[span_35](end_span)
            return False
    return True[span_36](start_span)[span_36](end_span)


def send_disaster_push(country_iso2, country_iso3, country_name, disaster_title, event_id, last_updated):
    topic_name = "all[span_37](start_span)"[span_37](end_span)

    if not last_updated:
        last_updated = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")[span_38](start_span)[span_38](end_span)
    elif not (last_updated.endswith('Z') or last_updated.endswith('z')):
        last_updated = str(last_updated).replace(" ", "T") + "Z[span_39](start_span)"[span_39](end_span)

    c_name = country_name if country_name else "Global[span_40](start_span)"[span_40](end_span)
    alarm_id = uuid.uuid4().hex[span_41](start_span)[span_41](end_span)

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
        )[span_42](start_span)[span_42](end_span)
        response = messaging.send(message)[span_43](start_span)[span_43](end_span)
        print(f"  👉 [FCM 알림 방송 성공] 토픽 채널: {topic_name} / 대상 국가: {c_name}({country_iso2}) / 알람ID: {alarm_id} (전송 ID: {response})")[span_44](start_span)[span_44](end_span)
    except Exception as e:
        print(f"  ❌ [FCM 알림 방송 실패] 토픽 채널: {topic_name} / 에러 내용: {e}")[span_45](start_span)[span_45](end_span)


def feature_key(feat):
    props = feat.get("properties", {}) or {}[span_46](start_span)[span_46](end_span)
    etype = props.get("eventtype", "")[span_47](start_span)[span_47](end_span)
    eid = props.get("eventid", "")[span_48](start_span)[span_48](end_span)
    if etype or eid:
        return f"{etype}{eid}[span_49](start_span)"[span_49](end_span)
    return None[span_50](start_span)[span_50](end_span)


def get_centroid(geom):
    if not isinstance(geom, dict):
        return None[span_51](start_span)[span_51](end_span)
    coords = geom.get("coordinates")[span_52](start_span)[span_52](end_span)
    if coords is None:
        return None[span_53](start_span)[span_53](end_span)

    pts = [][span_54](start_span)[span_54](end_span)
    stack = [coords][span_55](start_span)[span_55](end_span)

    while stack:
        curr = stack.pop()[span_56](start_span)[span_56](end_span)
        if not isinstance(curr, list) or not curr:
            continue[span_57](start_span)[span_57](end_span)

        if len(curr) >= 2 and all(isinstance(x, (int, float)) for x in curr[:2]):
            pts.append((float(curr[0]), float(curr[1])))[span_58](start_span)[span_58](end_span)
        else:
            stack.extend(curr)[span_59](start_span)[span_59](end_span)

    if not pts:
        return None[span_60](start_span)[span_60](end_span)

    lng = sum(p[0] for p in pts) / len(pts)[span_61](start_span)[span_61](end_span)
    lat = sum(p[1] for p in pts) / len(pts)[span_62](start_span)[span_62](end_span)
    return lng, lat[span_63](start_span)[span_63](end_span)


def fetch_json(url, timeout=5):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})[span_64](start_span)[span_64](end_span)
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))[span_65](start_span)[span_65](end_span)
    except Exception as e:
        print(f"  ⚠️ 상세정보 조회 실패: {url} ({e})")[span_66](start_span)[span_66](end_span)
        return None[span_67](start_span)[span_67](end_span)


def parse_gdacs_date(date_str):
    if not date_str:
        return None[span_68](start_span)[span_68](end_span)
    try:
        dt = datetime.fromisoformat(str(date_str).replace("Z", ""))[span_69](start_span)[span_69](end_span)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)[span_70](start_span)[span_70](end_span)
        return dt[span_71](start_span)[span_71](end_span)
    except ValueError:
        return None[span_72](start_span)[span_72](end_span)


def fetch_page_with_retry(url, retries=PAGE_RETRY_COUNT, delay=PAGE_RETRY_DELAY_SEC):
    status, body = None, None[span_73](start_span)[span_73](end_span)
    for attempt in range(1, retries + 1):
        try:
            req = urllib.request.Request(url, headers=HEADERS)[span_74](start_span)[span_74](end_span)
            with urllib.request.urlopen(req, timeout=15) as resp:
                status = resp.status[span_75](start_span)[span_75](end_span)
                body = resp.read()[span_76](start_span)[span_76](end_span)
        except Exception as e:
            print(f"    ⚠️ 페이지 요청 실패 (시도 {attempt}/{retries}): {e}")[span_77](start_span)[span_77](end_span)
            status, body = None, None[span_78](start_span)[span_78](end_span)

        if status == 200 and body:
            return status, body[span_79](start_span)[span_79](end_span)

        if attempt < retries:
            print(f"    ↻ 응답 이상 (status={status}, size={len(body) if body else 0}) "
                  f"- {delay}초 후 재시도 ({attempt}/{retries})")[span_80](start_span)[span_80](end_span)
            time.sleep(delay)[span_81](start_span)[span_81](end_span)

    return status, body[span_82](start_span)[span_82](end_span)


def fetch_events_for_type(event_type, max_pages=5):
    base_url = get_base_url(event_type)  # ⭐️ [개선] 날짜 범위가 포함된 URL 사용
    all_features = [][span_83](start_span)[span_83](end_span)
    seen_keys = set()[span_84](start_span)[span_84](end_span)
    prev_first_key = None[span_85](start_span)[span_85](end_span)

    for page in range(1, max_pages + 1):
        url = f"{base_url}&pagenumber={page}[span_86](start_span)"[span_86](end_span)
        status, body = fetch_page_with_retry(url)[span_87](start_span)[span_87](end_span)

        print(f"  [{event_type}] 페이지 {page} 응답 HTTP 상태코드: {status}, "
              f"크기: {len(body) if body else 0} bytes")[span_88](start_span)[span_88](end_span)

        if status != 200 or not body:
            print(f"  ⛔ [{event_type}] 페이지 {page} 재시도 {PAGE_RETRY_COUNT}회 모두 실패 - 이 타입 수집 중단")[span_89](start_span)[span_89](end_span)
            break

        try:
            page_json = json.loads(body)[span_90](start_span)[span_90](end_span)
        except Exception as e:
            print(f"  [{event_type}] 페이지 {page} JSON 파싱 실패: {e}")[span_91](start_span)[span_91](end_span)
            break

        page_features = page_json.get("features")[span_92](start_span)[span_92](end_span)
        if page_features is None:
            for alt_key in ("data", "results", "events", "FeatureCollection"):
                candidate = page_json.get(alt_key)[span_93](start_span)[span_93](end_span)
                if isinstance(candidate, dict):
                    candidate = candidate.get("features")[span_94](start_span)[span_94](end_span)
                if isinstance(candidate, list):
                    page_features = candidate[span_95](start_span)[span_95](end_span)
                    break

        if not page_features:
            break

        current_first_key = feature_key(page_features[0])[span_96](start_span)[span_96](end_span)
        if page > 1 and current_first_key is not None and current_first_key == prev_first_key:
            break
        prev_first_key = current_first_key[span_97](start_span)[span_97](end_span)

        new_in_page = 0[span_98](start_span)[span_98](end_span)
        for feat in page_features:
            key = feature_key(feat)[span_99](start_span)[span_99](end_span)
            if key is not None and key in seen_keys:
                continue[span_100](start_span)[span_100](end_span)
            if key is not None:
                seen_keys.add(key)[span_101](start_span)[span_101](end_span)
            all_features.append(feat)[span_102](start_span)[span_102](end_span)
            new_in_page += 1[span_103](start_span)[span_103](end_span)

        if new_in_page == 0:
            break

    return all_features[span_104](start_span)[span_104](end_span)


def fetch_disaster_list():
    all_features = [][span_105](start_span)[span_105](end_span)

    print("==================================================")[span_106](start_span)[span_106](end_span)
    print("🚀 실시간 재난 데이터 수집 가동 (GDACS API)")[span_107](start_span)[span_107](end_span)
    print("==================================================")[span_108](start_span)[span_108](end_span)

    for idx, event_type in enumerate(EVENT_TYPES, 1):
        print(f"\n📡 [{idx}/{len(EVENT_TYPES)}] GDACS [{event_type}] 수집 시작")[span_109](start_span)[span_109](end_span)
        type_features = fetch_events_for_type(event_type)[span_110](start_span)[span_110](end_span)
        print(f"  ✅ [{event_type}] {len(type_features)}건 수집 완료")[span_111](start_span)[span_111](end_span)
        all_features.extend(type_features)[span_112](start_span)[span_112](end_span)
        time.sleep(0.3)[span_113](start_span)[span_113](end_span)

    type_counts = Counter((f.get("properties", {}) or {}).get("eventtype", "?") for f in all_features)[span_114](start_span)[span_114](end_span)
    print(f"\n📊 수집된 원본 이벤트 타입 분포: {dict(type_counts)}")[span_115](start_span)[span_115](end_span)

    results = [][span_116](start_span)[span_116](end_span)
    skipped_not_current = 0[span_117](start_span)[span_117](end_span)
    skipped_duplicate = 0[span_118](start_span)[span_118](end_span)
    skipped_no_geom = 0[span_119](start_span)[span_119](end_span)
    skipped_too_old = 0[span_120](start_span)[span_120](end_span)
    seen_result_keys = set()[span_121](start_span)[span_121](end_span)
    cutoff_date = datetime.now(timezone.utc) - timedelta(days=DAYS_BACK)[span_122](start_span)[span_122](end_span)

    for feat in all_features:
        props = feat.get("properties", {}) or {}[span_123](start_span)[span_123](end_span)
        geom = feat.get("geometry", {}) or {}[span_124](start_span)[span_124](end_span)

        event_type_val = props.get("eventtype", "")[span_125](start_span)[span_125](end_span)
        event_id_val = props.get("eventid", "")[span_126](start_span)[span_126](end_span)

        # ⭐️ [개선] 불리언(True)과 문자열("true", "1", "yes") 등 다양한 파싱 결과 처리 지원
        raw_is_current = props.get("iscurrent")
        is_current_bool = (raw_is_current is True) or (str(raw_is_current).strip().lower() in ("true", "1", "yes"))

        if SHOW_ONLY_CURRENT and not is_current_bool:
            skipped_not_current += 1[span_127](start_span)[span_127](end_span)
            continue[span_128](start_span)[span_128](end_span)

        event_date = parse_gdacs_date(props.get("todate")) or parse_gdacs_date(props.get("fromdate"))[span_129](start_span)[span_129](end_span)
        if ENABLE_DATE_FILTER and event_date is not None and event_date < cutoff_date:
            skipped_too_old += 1[span_130](start_span)[span_130](end_span)
            continue[span_131](start_span)[span_131](end_span)

        if event_type_val and event_id_val:
            result_key = f"{event_type_val}{event_id_val}[span_132](start_span)"[span_132](end_span)
            if result_key in seen_result_keys:
                skipped_duplicate += 1[span_133](start_span)[span_133](end_span)
                continue[span_134](start_span)[span_134](end_span)
            seen_result_keys.add(result_key)[span_135](start_span)[span_135](end_span)

        centroid = get_centroid(geom)[span_136](start_span)[span_136](end_span)
        if centroid is None:
            skipped_no_geom += 1[span_137](start_span)[span_137](end_span)
            continue[span_138](start_span)[span_138](end_span)
        lng, lat = centroid[span_139](start_span)[span_139](end_span)

        if not (-180 <= lng <= 180 and -90 <= lat <= 90):
            skipped_no_geom += 1[span_140](start_span)[span_140](end_span)
            continue[span_141](start_span)[span_141](end_span)

        raw_country = (props.get("country") or "").strip()[span_142](start_span)[span_142](end_span)
        clean_country = GDACS_COUNTRY_MAP.get(raw_country.lower(), raw_country)[span_143](start_span)[span_143](end_span)

        event_name = props.get("eventname") or props.get("name") or props.get("eventtype", "Disaster")[span_144](start_span)[span_144](end_span)
        title = f"{event_name} - {clean_country}" if clean_country else event_name[span_145](start_span)[span_145](end_span)

        desc = props.get("description") or props.get("htmldescription") or "[span_146](start_span)"[span_146](end_span)
        desc_clean = re.sub(r'<[^>]*>', '', desc).strip()[span_147](start_span)[span_147](end_span)

        if not desc_clean:
            severity_str = (props.get("alertlevel") or "unknown").upper()[span_148](start_span)[span_148](end_span)
            desc_clean = f"A {severity_str} level {event_name} event has been detected near {clean_country or 'coordinates'}: {lat}, {lng}.[span_149](start_span)"[span_149](end_span)

        api_report_url = (props.get("url") or {}).get("report", "") if isinstance(props.get("url"), dict) else "[span_150](start_span)"[span_150](end_span)
        if api_report_url:
            report_url = api_report_url[span_151](start_span)[span_151](end_span)
        elif event_type_val and event_id_val:
            report_url = f"https://www.gdacs.org/report.aspx?eventtype={event_type_val}&eventid={event_id_val}[span_152](start_span)"[span_152](end_span)
        else:
            report_url = "[span_153](start_span)"[span_153](end_span)

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
            "is_current": is_current_bool,
        })[span_154](start_span)[span_154](end_span)

    print(f"\n⚙️  총 {len(results)}건 재난 추출 완료 (스킵: {skipped_no_geom}좌표누락, {skipped_not_current}비활성, {skipped_duplicate}중복, {skipped_too_old}기간초과)")[span_155](start_span)[span_155](end_span)

    print(f"\n✂️  각 타입별 기준일 이내 갱신된 진행중 재난만 유지")[span_156](start_span)[span_156](end_span)
    categorized = {etype: [] for etype in EVENT_TYPES}[span_157](start_span)[span_157](end_span)

    for r in results:
        etype = r.get("eventtype")[span_158](start_span)[span_158](end_span)
        if etype in categorized:
            categorized[etype].append(r)[span_159](start_span)[span_159](end_span)

    now_utc = datetime.now(timezone.utc)[span_160](start_span)[span_160](end_span)
    filtered_results = [][span_161](start_span)[span_161](end_span)

    for etype, items in categorized.items():
        def _get_date_key(x):
            dt = parse_gdacs_date(x.get("last_updated"))[span_162](start_span)[span_162](end_span)
            return dt or datetime.min.replace(tzinfo=timezone.utc)[span_163](start_span)[span_163](end_span)

        max_age_days = EVENT_TYPE_MAX_AGE_DAYS_BY_TYPE.get(etype, DEFAULT_EVENT_TYPE_MAX_AGE_DAYS)[span_164](start_span)[span_164](end_span)
        age_cutoff = now_utc - timedelta(days=max_age_days)[span_165](start_span)[span_165](end_span)

        recent_items = [it for it in items if _get_date_key(it) >= age_cutoff][span_166](start_span)[span_166](end_span)
        recent_items.sort(key=_get_date_key, reverse=True)[span_167](start_span)[span_167](end_span)

        filtered_results.extend(recent_items)[span_168](start_span)[span_168](end_span)
        dropped = len(items) - len(recent_items)[span_169](start_span)[span_169](end_span)
        print(f"  • [{etype}] 총 {len(items)}건 중 최근 {len(recent_items)}건 유지 (기준: {max_age_days}일, {dropped}건은 기준일 초과로 제외)")[span_170](start_span)[span_170](end_span)

    final_counts = Counter(r.get("eventtype") for r in filtered_results)[span_171](start_span)[span_171](end_span)
    print(f"👉 필터링 후 최종 결과 타입별 분포: {dict(final_counts)}")[span_172](start_span)[span_172](end_span)
    print(f"👉 총 수집 대상 목록 수: {len(filtered_results)}건")[span_173](start_span)[span_173](end_span)

    return filtered_results[span_174](start_span)[span_174](end_span)


def process_single_enrich(r):
    r_eventtype = r.get("eventtype") or r.get("event_type") or "[span_175](start_span)"[span_175](end_span)
    r_alertlevel = str(r.get("alert_level", "green")).lower()[span_176](start_span)[span_176](end_span)
    r["report_description"] = (
        f"This {EVENT_TYPE_NAME.get(r_eventtype, 'event')} could have a "
        f"{IMPACT_LEVEL.get(r_alertlevel, 'unknown')} impact on affected communities, "
        f"based on {IMPACT_BASIS.get(r_eventtype, 'the severity')} and the "
        f"exposure and vulnerability of the population nearby."
    )[span_177](start_span)[span_177](end_span)

    eventtype = r.get("eventtype") or r.get("event_type")[span_178](start_span)[span_178](end_span)
    eventid = r.get("eventid")[span_179](start_span)[span_179](end_span)

    if not eventid:
        m = re.search(r"(\d+)$", str(r.get("gdacs_id", "")))[span_180](start_span)[span_180](end_span)
        if m:
            eventid = m.group(1)[span_181](start_span)[span_181](end_span)

    if not eventtype or not eventid:
        return r, False[span_182](start_span)[span_182](end_span)

    detail_url = f"https://www.gdacs.org/gdacsapi/api/events/geteventdata?eventtype={eventtype}&eventid={eventid}[span_183](start_span)"[span_183](end_span)
    detail = fetch_json(detail_url)[span_184](start_span)[span_184](end_span)

    if not detail:
        return r, False[span_185](start_span)[span_185](end_span)

    props = detail.get("properties", detail) or {}[span_186](start_span)[span_186](end_span)
    sendai = props.get("sendai") or [][span_187](start_span)[span_187](end_span)
    severity = props.get("severitydata") or {}[span_188](start_span)[span_188](end_span)
    images = props.get("images") or {}[span_189](start_span)[span_189](end_span)
    eq_details = props.get("earthquakedetails") or {}[span_190](start_span)[span_190](end_span)

    deaths = 0[span_191](start_span)[span_191](end_span)
    displaced = 0[span_192](start_span)[span_192](end_span)
    missing = 0[span_193](start_span)[span_193](end_span)
    deaths_found = False[span_194](start_span)[span_194](end_span)
    displaced_found = False[span_195](start_span)[span_195](end_span)
    missing_found = False[span_196](start_span)[span_196](end_span)
    sendai_details = [][span_197](start_span)[span_197](end_span)

    for s in sendai:
        name = (s.get("sendainame") or "").lower()[span_198](start_span)[span_198](end_span)
        try:
            val = int(re.sub(r"[^\d]", "", str(s.get("sendaivalue", "0"))) or 0)[span_199](start_span)[span_199](end_span)
        except ValueError:
            val = 0[span_200](start_span)[span_200](end_span)

        is_latest = str(s.get("latest", "")).strip().lower() in ("true", "1", "yes")[span_201](start_span)[span_201](end_span)

        if is_latest:
            if "death" in name:
                deaths += val[span_202](start_span)[span_202](end_span)
                deaths_found = True[span_203](start_span)[span_203](end_span)
            elif "displaced" in name or "evacuat" in name:
                displaced += val[span_204](start_span)[span_204](end_span)
                displaced_found = True[span_205](start_span)[span_205](end_span)
            elif "missing" in name:
                missing += val[span_206](start_span)[span_206](end_span)
                missing_found = True[span_207](start_span)[span_207](end_span)

        sendai_details.append({
            "type": s.get("sendaitype"),
            "name": s.get("sendainame"),
            "value": s.get("sendaivalue"),
            "region": s.get("region"),
            "description": s.get("description"),
            "date": s.get("dateinsert"),
            "latest": s.get("latest"),
        })[span_208](start_span)[span_208](end_span)

    r["deaths"] = deaths if deaths_found else None[span_209](start_span)[span_209](end_span)
    r["displaced"] = displaced if displaced_found else None[span_210](start_span)[span_210](end_span)
    r["missing"] = missing if missing_found else None[span_211](start_span)[span_211](end_span)

    r["severity_text"] = severity.get("severitytext")[span_212](start_span)[span_212](end_span)
    r["impact_history"] = sendai_details[span_213](start_span)[span_213](end_span)
    r["impact_description"] = (sendai_details[-1]["description"][:300] if sendai_details else None)[span_214](start_span)[span_214](end_span)

    detail_desc_candidates = [
        props.get("description"),
        props.get("htmldescription"),
        props.get("eventdescription"),
        props.get("longdescription"),
        detail.get("description") if isinstance(detail, dict) else None,
        detail.get("htmldescription") if isinstance(detail, dict) else None,
    ][span_215](start_span)[span_215](end_span)
    detail_description = None[span_216](start_span)[span_216](end_span)
    for candidate in detail_desc_candidates:
        if not candidate:
            continue[span_217](start_span)[span_217](end_span)
        cleaned = re.sub(r'<[^>]*>', '', str(candidate)).strip()[span_218](start_span)[span_218](end_span)
        if cleaned and cleaned.lower() != str(r.get("title", "")).lower():
            detail_description = cleaned[span_219](start_span)[span_219](end_span)
            break[span_220](start_span)[span_220](end_span)
    r["detail_description"] = detail_description[span_221](start_span)[span_221](end_span)

    image_urls = [][span_222](start_span)[span_222](end_span)
    if isinstance(images, dict):
        for key, val in images.items():
            if isinstance(val, str) and val.strip().lower().startswith(("http://", "https://")):
                image_urls.append(val.strip())[span_223](start_span)[span_223](end_span)
    r["image_urls"] = image_urls[span_224](start_span)[span_224](end_span)
    r["overview_map_url"] = images.get("overviewmap") or images.get("overviewmap_cached") or (image_urls[0] if image_urls else None)[span_225](start_span)[span_225](end_span)
    r["report_detail_url"] = detail_url[span_226](start_span)[span_226](end_span)

    r["magnitude"] = eq_details.get("magnitude")[span_227](start_span)[span_227](end_span)
    r["depth_km"] = eq_details.get("depth")[span_228](start_span)[span_228](end_span)
    r["event_date_local"] = eq_details.get("episodedatelocal")[span_229](start_span)[span_229](end_span)
    r["exposed_population"] = eq_details.get("rapidpop")[span_230](start_span)[span_230](end_span)
    r["exposed_population_description"] = eq_details.get("rapidpopdescription")[span_231](start_span)[span_231](end_span)

    return r, True[span_232](start_span)[span_232](end_span)


def enrich_disasters_parallel(results):
    results.sort(key=lambda r: SEVERITY_ORDER.get(str(r.get("alert_level", "green")).lower(), 3))[span_233](start_span)[span_233](end_span)

    print(f"\n🔄 [병렬 스레드 적용] 상세 정보(Enrich) 수집 시작... (총 {len(results)}개 대상, Workers: {MAX_WORKERS})")[span_234](start_span)[span_234](end_span)

    enriched_results = [][span_235](start_span)[span_235](end_span)
    enriched_ok = 0[span_236](start_span)[span_236](end_span)
    total_count = len(results)[span_237](start_span)[span_237](end_span)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_single_enrich, r): r for r in results}[span_238](start_span)[span_238](end_span)

        for idx, future in enumerate(as_completed(futures), 1):
            updated_record, success = future.result()[span_239](start_span)[span_239](end_span)
            enriched_results.append(updated_record)[span_240](start_span)[span_240](end_span)
            if success:
                enriched_ok += 1[span_241](start_span)[span_241](end_span)

            etype = updated_record.get("eventtype", "?")[span_242](start_span)[span_242](end_span)
            eid = updated_record.get("eventid", "?")[span_243](start_span)[span_243](end_span)
            status_text = "성공" if success else "실패[span_244](start_span)"[span_244](end_span)
            print(f"  ⚡ [{idx}/{total_count}] 완료: {etype}{eid} ({status_text})")[span_245](start_span)[span_245](end_span)

    print(f"\n🎉 상세정보 API 호출 {total_count}건 중 {enriched_ok}건 완벽 보강 완료!")[span_246](start_span)[span_246](end_span)
    return enriched_results[span_247](start_span)[span_247](end_span)


def load_sent_ids_history():
    file_existed = os.path.exists(SENT_IDS_FILEPATH)[span_248](start_span)[span_248](end_span)
    if not file_existed:
        return {}, False[span_249](start_span)[span_249](end_span)

    try:
        with open(SENT_IDS_FILEPATH, "r", encoding="utf-8") as f:
            raw = json.load(f)[span_250](start_span)[span_250](end_span)
        history = raw.get("history", {})[span_251](start_span)[span_251](end_span)
    except Exception as e:
        print(f"⚠️ 발송 이력 파일 파싱 스킵 (파일 깨짐 포착): {e}")[span_252](start_span)[span_252](end_span)
        return {}, False[span_253](start_span)[span_253](end_span)

    prune_cutoff = datetime.now(timezone.utc) - timedelta(days=PRUNE_AFTER_DAYS)[span_254](start_span)[span_254](end_span)
    pruned = {}[span_255](start_span)[span_255](end_span)
    for gdacs_id, entry in history.items():
        recorded_at = parse_gdacs_date(entry.get("recorded_at"))[span_256](start_span)[span_256](end_span)
        if recorded_at is not None and recorded_at < prune_cutoff:
            continue[span_257](start_span)[span_257](end_span)
        pruned[gdacs_id] = entry[span_258](start_span)[span_258](end_span)

    dropped = len(history) - len(pruned)[span_259](start_span)[span_259](end_span)
    print(f"🔍 발송 이력 파일 검사 완료: {len(pruned)}건 유지 ({dropped}건은 {PRUNE_AFTER_DAYS}일 초과로 정리)")[span_260](start_span)[span_260](end_span)
    return pruned, True[span_261](start_span)[span_261](end_span)


def save_sent_ids_history(history):
    tmp_path = SENT_IDS_FILEPATH + ".tmp[span_262](start_span)"[span_262](end_span)
    try:
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump({"history": history}, f, ensure_ascii=False, indent=2)[span_263](start_span)[span_263](end_span)
        os.replace(tmp_path, SENT_IDS_FILEPATH)[span_264](start_span)[span_264](end_span)
        print(f"💾 발송 이력 파일 갱신 완료: 총 {len(history)}건 기록")[span_265](start_span)[span_265](end_span)
    except Exception as e:
        print(f"⚠️ 발송 이력 파일 저장 실패: {e}")[span_266](start_span)[span_266](end_span)
        if os.path.exists(tmp_path):
            os.remove(tmp_path)[span_267](start_span)[span_267](end_span)


def main():
    results = fetch_disaster_list()[span_268](start_span)[span_268](end_span)

    if not results:
        print("\n🚨 [위험] 수집 및 필터링 완료된 데이터가 총 0건입니다.")[span_269](start_span)[span_269](end_span)
        print("💡 API 서버 장애 혹은 네트워크 타임아웃으로 예상되며, 기존 JSON 데이터를 보호하기 위해 프로그램 쓰기를 건너뛰고 정상 안전 종료합니다.")[span_270](start_span)[span_270](end_span)
        sys.exit(0)[span_271](start_span)[span_271](end_span)

    results = enrich_disasters_parallel(results)[span_272](start_span)[span_272](end_span)

    temp_filepath = "data/realtime_disasters.json.tmp[span_273](start_span)"[span_273](end_span)
    final_filepath = "data/realtime_disasters.json[span_274](start_span)"[span_274](end_span)

    os.makedirs(os.path.dirname(final_filepath), exist_ok=True)[span_275](start_span)[span_275](end_span)

    sent_history, history_existed = load_sent_ids_history()[span_276](start_span)[span_276](end_span)

    is_fcm_ready = init_firebase()[span_277](start_span)[span_277](end_span)

    if is_fcm_ready:
        now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")[span_278](start_span)[span_278](end_span)

        if not history_existed:
            print("\n📢 [최초 실행 감지] 발송 이력 파일이 없어 현재 활성 재난을 이력에만 기록하고, 이번 회차는 푸시를 발송하지 않습니다.")[span_279](start_span)[span_279](end_span)
            for r in results:
                gdacs_id = r.get("gdacs_id")[span_280](start_span)[span_280](end_span)
                if gdacs_id:
                    sent_history[gdacs_id] = {
                        "last_updated": r.get("last_updated", ""),
                        "fingerprint": compute_disaster_fingerprint(r),
                        "recorded_at": now_iso
                    }[span_281](start_span)[span_281](end_span)
        else:
            print("\n📢 [정기 실행: 신규/갱신 재난 판별 및 푸시 알림 프로세스 가동]")[span_282](start_span)[span_282](end_span)
            new_disaster_count = 0[span_283](start_span)[span_283](end_span)

            for r in results:
                gdacs_id = r.get("gdacs_id")[span_284](start_span)[span_284](end_span)
                if not gdacs_id:
                    continue[span_285](start_span)[span_285](end_span)

                current_last_updated = r.get("last_updated", "")[span_286](start_span)[span_286](end_span)
                current_fingerprint = compute_disaster_fingerprint(r)[span_287](start_span)[span_287](end_span)
                prior_entry = sent_history.get(gdacs_id)[span_288](start_span)[span_288](end_span)

                is_new_or_updated = (prior_entry is None) or (prior_entry.get("fingerprint") != current_fingerprint)[span_289](start_span)[span_289](end_span)

                if is_new_or_updated:
                    new_disaster_count += 1[span_290](start_span)[span_290](end_span)

                    iso3_val = str(r.get("iso3", "")).upper().strip()[span_291](start_span)[span_291](end_span)
                    country_name = r.get("country", "Global")[span_292](start_span)[span_292](end_span)
                    iso2_backup = iso3_to_iso2(iso3_val)[span_293](start_span)[span_293](end_span)

                    reason = "신규" if prior_entry is None else "갱신[span_294](start_span)"[span_294](end_span)
                    print(f"  🆕 [{reason} 재난 포착] 제목: {r.get('title')} (ID: {gdacs_id})")[span_295](start_span)[span_295](end_span)

                    send_disaster_push(
                        country_iso2=iso2_backup,
                        country_iso3=iso3_val,
                        country_name=country_name,
                        disaster_title=r.get("title", "재난 경보"),
                        event_id=gdacs_id,
                        last_updated=current_last_updated
                    )[span_296](start_span)[span_296](end_span)

                    sent_history[gdacs_id] = {
                        "last_updated": current_last_updated,
                        "fingerprint": current_fingerprint,
                        "recorded_at": now_iso
                    }[span_297](start_span)[span_297](end_span)

                    save_sent_ids_history(sent_history)[span_298](start_span)[span_298](end_span)

            if new_disaster_count == 0:
                print("  - 지난 회차 대비 새롭게 발생하거나 갱신된 재난 정보가 없으므로 알림 발송 처리를 안전하게 패스합니다.")[span_299](start_span)[span_299](end_span)
    else:
        print("\n⚠️ 파이어베이스 작동에 필요한 Secrets 값이 없으므로 신규 재난 비교 및 FCM 전송 엔진을 가동하지 않습니다.")[span_300](start_span)[span_300](end_span)

    save_sent_ids_history(sent_history)[span_301](start_span)[span_301](end_span)

    try:
        with open(temp_filepath, "w", encoding="utf-8") as f:
            json.dump({"status": "success", "data": results}, f, ensure_ascii=False, indent=2)[span_302](start_span)[span_302](end_span)

        os.replace(temp_filepath, final_filepath)[span_303](start_span)[span_303](end_span)
        print(f"\n💾 파일 원자적 저장 성공: '{final_filepath}' 업데이트 완료!")[span_304](start_span)[span_304](end_span)
    except Exception as e:
        print(f"\n❌ 파일 쓰기 오류 발생: {e}")[span_305](start_span)[span_305](end_span)
        if os.path.exists(temp_filepath):
            os.remove(temp_filepath)[span_306](start_span)[span_306](end_span)
        sys.exit(1)[span_307](start_span)[span_307](end_span)


if __name__ == "__main__":
    main()[span_308](start_span)[span_308](end_span)
