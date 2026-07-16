import json
import re
import time
import urllib.request
import urllib.error

def fetch_json(url, timeout=15):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        print(f"  ⚠️ 상세정보 조회 실패: {url} ({e})")
        return None

def main():
    with open("data/realtime_disasters.json", encoding="utf-8") as f:
        payload = json.load(f)

    results = payload.get("data", [])

    SEVERITY_ORDER = {"red": 0, "orange": 1, "green": 2}
    results.sort(key=lambda r: SEVERITY_ORDER.get(str(r.get("alert_level", "green")).lower(), 3))

    enrich_count = 0
    enriched_ok = 0

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

    for r in results:
        r_eventtype = r.get("eventtype") or r.get("event_type") or ""
        r_alertlevel = str(r.get("alert_level", "green")).lower()
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
        detail = fetch_json(detail_url)
        enrich_count += 1
        time.sleep(0.6)  

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

    print(f"상세정보 API 호출 {enrich_count}건 중 {enriched_ok}건 보강 완료")
    with open("data/realtime_disasters.json", "w", encoding="utf-8") as f:
        json.dump({"status": "success", "data": results}, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    main()
