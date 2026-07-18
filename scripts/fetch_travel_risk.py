import json
import os
import re
import unicodedata
import xml.etree.ElementTree as ET

NAME_TO_ISO2 = {
    "afghanistan": "AF", "albania": "AL", "algeria": "DZ", "andorra": "AD", "angola": "AO",
    "antigua and barbuda": "AG", "argentina": "AR", "armenia": "AM", "australia": "AU",
    "austria": "AT", "azerbaijan": "AZ", "bahamas": "BS", "the bahamas": "BS", "bahrain": "BH",
    "bangladesh": "BD", "barbados": "BB", "belarus": "BY", "belgium": "BE", "belize": "BZ",
    "benin": "BJ", "bhutan": "BT", "bolivia": "BO", "bosnia and herzegovina": "BA",
    "botswana": "BW", "brazil": "BR", "brunei": "BN", "brunei darussalam": "BN",
    "bulgaria": "BG", "burkina faso": "BF", "burma": "MM", "burma (myanmar)": "MM",
    "myanmar": "MM", "burundi": "BI", "cabo verde": "CV", "cape verde": "CV",
    "cambodia": "KH", "cameroon": "CM", "canada": "CA", "central african republic": "CF",
    "chad": "TD", "chile": "CL", "china": "CN", "mainland china": "CN",
    "colombia": "CO", "comoros": "KM", "democratic republic of the congo": "CD",
    "dr congo": "CD", "congo-kinshasa": "CD", "republic of the congo": "CG",
    "congo-brazzaville": "CG", "costa rica": "CR", "cote d ivoire": "CI",
    "cote d'ivoire": "CI", "ivory coast": "CI", "croatia": "HR",
    "cuba": "CU", "cyprus": "CY", "czech republic": "CZ", "czechia": "CZ",
    "denmark": "DK", "kingdom of denmark": "DK", "djibouti": "DJ", "dominica": "DM",
    "dominican republic": "DO", "ecuador": "EC", "egypt": "EG", "el salvador": "SV",
    "equatorial guinea": "GQ", "eritrea": "ER", "estonia": "EE", "eswatini": "SZ",
    "swaziland": "SZ", "ethiopia": "ET", "fiji": "FJ", "finland": "FI", "france": "FR",
    "gabon": "GA", "gambia": "GM", "the gambia": "GM", "georgia": "GE", "germany": "DE",
    "ghana": "GH", "greece": "GR", "grenada": "GD", "guatemala": "GT", "guinea": "GN",
    "guinea-bissau": "GW", "guyana": "GY", "haiti": "HT", "honduras": "HN",
    "hong kong": "HK", "hungary": "HU", "iceland": "IS", "india": "IN", "indonesia": "ID",
    "iran": "IR", "iraq": "IQ", "ireland": "IE", "israel": "IL", "italy": "IT",
    "jamaica": "JM", "japan": "JP", "jordan": "JO", "kazakhstan": "KZ", "kenya": "KE",
    "kiribati": "KI", "north korea": "KP", "kosovo": "XK", "kuwait": "KW",
    "kyrgyz republic": "KG", "kyrgyzstan": "KG", "the kyrgyz republic": "KG",
    "laos": "LA", "lao people's democratic republic": "LA", "latvia": "LV",
    "lebanon": "LB", "lesotho": "LS", "liberia": "LR", "libya": "LY",
    "liechtenstein": "LI", "lithuania": "LT", "luxembourg": "LU", "macau": "MO",
    "madagascar": "MG", "malawi": "MW", "malaysia": "MY", "maldives": "MV", "mali": "ML",
    "malta": "MT", "marshall islands": "MH", "mauritania": "MR", "mauritius": "MU",
    "mexico": "MX", "micronesia": "FM", "federated states of micronesia": "FM",
    "moldova": "MD", "republic of moldova": "MD", "monaco": "MC", "mongolia": "MN",
    "montenegro": "ME", "montserrat": "MS", "morocco": "MA", "mozambique": "MZ",
    "namibia": "NA", "nauru": "NR", "nepal": "NP", "netherlands": "NL",
    "new zealand": "NZ", "nicaragua": "NI", "niger": "NE", "nigeria": "NG",
    "north macedonia": "MK", "macedonia": "MK", "norway": "NO", "the kingdom of norway": "NO",
    "oman": "OM", "pakistan": "PK", "palau": "PW", "panama": "PA", "papua new guinea": "PG",
    "paraguay": "PY", "peru": "PE", "philippines": "PH", "poland": "PL",
    "portugal": "PT", "qatar": "QA", "romania": "RO", "russia": "RU",
    "russian federation": "RU", "rwanda": "RW", "saint kitts and nevis": "KN",
    "saint lucia": "LC", "saint vincent and the grenadines": "VC", "samoa": "WS",
    "san marino": "SM", "sao tome and principe": "ST", "saudi arabia": "SA",
    "senegal": "SN", "serbia": "RS", "seychelles": "SC", "sierra leone": "SL",
    "singapore": "SG", "slovakia": "SK", "slovenia": "SI", "solomon islands": "SB",
    "somalia": "SO", "south africa": "ZA", "south korea": "KR", "republic of korea": "KR",
    "south sudan": "SS", "spain": "ES", "sri lanka": "LK", "sudan": "SD", "suriname": "SR",
    "sweden": "SE", "switzerland": "CH", "swiss confederation": "CH", "syria": "SY",
    "syrian arab republic": "SY", "taiwan": "TW", "tajikistan": "TJ", "tanzania": "TZ",
    "united republic of tanzania": "TZ", "thailand": "TH", "timor-leste": "TL",
    "east timor": "TL", "togo": "TG", "tonga": "TO", "trinidad and tobago": "TT",
    "tunisia": "TN", "turkey": "TR", "turkiye": "TR", "turkmenistan": "TM",
    "tuvalu": "TV", "uganda": "UG", "ukraine": "UA", "united arab emirates": "AE",
    "united kingdom": "GB", "united states": "US", "uruguay": "UY", "uzbekistan": "UZ",
    "vanuatu": "VU", "vatican city": "VA", "holy see": "VA", "venezuela": "VE",
    "vietnam": "VN", "viet nam": "VN", "yemen": "YE", "zambia": "ZM", "zimbabwe": "ZW",
    "french guiana": "GF", "turks and caicos islands": "TC", "british virgin islands": "VG",
    "anguilla": "AI", "new caledonia": "NC", "cayman islands": "KY", "bermuda": "BM",
    "french polynesia": "PF", "mainland china, hong kong & macau": "CN", "greenland": "GL",
    "curacao": "CW", "aruba": "AW", "sint maarten": "SX", "sint eustatius": "BQ",
    "bonaire": "BQ", "saba": "BQ",
    "gibraltar": "GI", "guernsey": "GG", "jersey": "JE", "isle of man": "IM",
    "falkland islands": "FK", "the falkland islands": "FK",
}

LEVEL_RE = re.compile(r"level\s*([1-4])", re.IGNORECASE)

OUTPUT_PATH = "data/travel_risk.json"
UNMATCHED_LOG_PATH = "data/unmatched_countries.log"


def normalize(name):
    name = name.replace("\u2019", "'").replace("\u2018", "'")
    name = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode()
    name = name.lower().strip()
    name = re.sub(r"\s+", " ", name)
    if name.startswith("the "):
        name = name[4:]
    name = name.replace(",", "").replace(".", "")
    return name


def extract_countries(country_part):
    whole_key = normalize(country_part)
    if whole_key in NAME_TO_ISO2:
        return [country_part]

    parts = re.split(r'\s+and\s+|\s*,\s*', country_part, flags=re.IGNORECASE)
    valid_countries = []
    for p in parts:
        p_clean = p.strip()
        if p_clean and p_clean.lower() not in ['and', 'or', 'the']:
            valid_countries.append(p_clean)
    return valid_countries if valid_countries else [country_part]


def load_existing_results():
    """
    이전 실행에서 저장된 travel_risk.json을 불러와 iso_code -> row 딕셔너리로 반환.
    파일이 없거나 형식이 깨져 있으면 빈 딕셔너리를 반환한다 (첫 실행 대비).
    """
    if not os.path.exists(OUTPUT_PATH):
        return {}
    try:
        with open(OUTPUT_PATH, "r", encoding="utf-8") as f:
            payload = json.load(f)
        existing = {}
        for row in payload.get("data", []):
            iso = row.get("iso_code")
            if iso:
                existing[iso] = row
        return existing
    except (json.JSONDecodeError, OSError):
        # 이전 파일이 손상되어 있어도 파이프라인이 멈추지 않도록 빈 상태로 시작
        return {}


def main():
    tree = ET.parse("state_dept_raw.xml")
    root = tree.getroot()

    # 국무부 RSS(TAsTWs.xml)는 전체 국가의 영구 스냅샷이 아니라
    # "최근 검토/갱신된" 항목만 담는 롤링(rolling) 피드다.
    # 그래서 이번 실행에서 못 받은 국가라고 해서 광고 자체가 사라진 게 아니다.
    # -> 매번 새로 덮어쓰지 않고, 기존 결과에 이번 실행 결과를 upsert(병합)한다.
    existing_results = load_existing_results()
    merged_results = dict(existing_results)  # iso_code -> row

    unmatched = []
    seen_iso2_this_run = set()
    total_items = len(list(root.iter("item")))
    print(f"RSS 원본 item 개수: {total_items}")

    for item in root.iter("item"):
        title_el = item.find("title")
        pubdate_el = item.find("pubDate")
        if title_el is None or not title_el.text:
            continue

        title = title_el.text.strip()
        split_match = re.search(r"\s[-–—]\s", title)
        if not split_match:
            unmatched.append(f"{title} (구분자 미발견)")
            continue

        raw_country_part = title[:split_match.start()]
        level_part = title[split_match.end():]
        raw_country_part = re.sub(r"\s+Travel Advisory$", "", raw_country_part).strip()

        level_match = LEVEL_RE.search(level_part)
        if not level_match:
            unmatched.append(f"{title} (Level 패턴 미발견: '{level_part.strip()[:60]}')")
            continue
        level = int(level_match.group(1))

        parsed_countries = extract_countries(raw_country_part)

        for country_name in parsed_countries:
            key = normalize(country_name)
            iso2 = NAME_TO_ISO2.get(key)

            if not iso2:
                unmatched.append(country_name)
                continue

            if iso2 in seen_iso2_this_run:
                continue
            seen_iso2_this_run.add(iso2)

            # upsert: 이번 실행에서 확인된 국가는 최신 정보로 덮어씀
            merged_results[iso2] = {
                "iso_code": iso2,
                "name": country_name,
                "advisory_level": level,
                "risk_score": 0.0,
                "last_updated": pubdate_el.text if pubdate_el is not None else "",
            }

    if "US" not in merged_results:
        merged_results["US"] = {
            "iso_code": "US",
            "name": "United States",
            "advisory_level": 1,
            "risk_score": 0.0,
            "last_updated": "RSS Default Protection"
        }

    skipped_countries = set(existing_results.keys()) - seen_iso2_this_run
    if skipped_countries:
        print(
            f"이번 실행 피드에 없어 기존 값 유지한 국가 수: {len(skipped_countries)} "
            f"(예: {sorted(skipped_countries)[:5]})"
        )

    os.makedirs("data", exist_ok=True)

    with open(UNMATCHED_LOG_PATH, "w", encoding="utf-8") as f:
        if unmatched:
            f.write("\n".join(unmatched) + "\n")

    results = list(merged_results.values())
    print(f"총 {len(results)}개국 위험도 추출 완료 (이번 실행 신규/갱신: {len(seen_iso2_this_run)}개)")
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump({"status": "success", "data": results}, f, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    main()
