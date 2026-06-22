import os
import requests
import zipfile
import pandas as pd
import json
import shutil

OUTPUT_DIR = "m/output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

MOBILITY_TOKEN = os.environ["MOBILITY_TOKEN"]
MDB_TOKEN_URL = "https://api.mobilitydatabase.org/v1/tokens"
MDB_FEEDS_URL = "https://api.mobilitydatabase.org/v1/gtfs_feeds"

def get_access_token() -> str:
    resp = requests.post(
        MDB_TOKEN_URL,
        headers={"Content-Type": "application/json"},
        json={"refresh_token": MOBILITY_TOKEN},
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]

def get_taiwan_feed_url(access_token: str) -> str:
    """
    💡 [핵심 수정] 버스 연합 피드를 피하고, 
    지하철(MRT) 및 국철(TRA)이 포함된 대만 공식/대도시 교통 피드를 정밀 검색합니다.
    """
    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {access_token}",
    }
    params = {"country_code": "TW", "limit": 100, "offset": 0}
    resp = requests.get(MDB_FEEDS_URL, headers=headers, params=params, timeout=15)
    resp.raise_for_status()
    feeds = resp.json()

    # 1순위: 타이베이 메트로(TAIPEI) 또는 대만 교통부 공식 통합 데이터(MOTC, PTX) 검색
    for feed in feeds:
        provider = str(feed.get("provider", "")).upper()
        producer_url = (feed.get("source_info") or {}).get("producer_url", "")
        
        if "TAIPEI" in provider or "MOTC" in provider or "PTX" in provider:
            if producer_url:
                print(f"🎯 메트로/철도 포함 대만 공식 피드 매칭 완료: {feed.get('id')} / {provider}")
                return producer_url

    # 2순위: 위 조건이 없으면 주소값이 존재하는 첫 번째 대만 피드로 대체 백업
    for feed in feeds:
        producer_url = (feed.get("source_info") or {}).get("producer_url", "")
        if producer_url:
            print(f"ℹ️ 공식 메트로 피드 부재로 일반 대만 피드 선택: {feed.get('id')}")
            return producer_url
            
    raise RuntimeError("대만 피드를 찾을 수 없습니다.")

def process_taiwan_gtfs():
    city_id      = "taiwan"
    zip_path     = f"{city_id}_gtfs.zip"
    extract_path = f"{city_id}_extracted"

    print("[1/4] access token 발급 중...")
    access_token = get_access_token()

    print("[2/4] 대만 메트로/철도 피드 타겟 검색 중...")
    feed_url = get_taiwan_feed_url(access_token)
    print(f"🚀 다운로드 URL 확정: {feed_url}")

    print("[3/4] GTFS zip 파일 다운로드 중...")
    resp = requests.get(feed_url, stream=True, timeout=90)
    resp.raise_for_status()
    with open(zip_path, "wb") as f:
        for chunk in resp.iter_content(chunk_size=8192):
            f.write(chunk)

    print("[4/4] 압축 해제 및 지하철 노선망 GeoJSON 생성 중...")
    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            zf.extractall(extract_path)
    except zipfile.BadZipFile:
        print("❌ 유효하지 않은 GTFS ZIP 파일입니다.")
        return
    finally:
        if os.path.exists(zip_path):
            os.remove(zip_path)

    routes_p = os.path.join(extract_path, "routes.txt")
    trips_p  = os.path.join(extract_path, "trips.txt")
    shapes_p = os.path.join(extract_path, "shapes.txt")

    if not (os.path.exists(routes_p) and os.path.exists(trips_p) and os.path.exists(shapes_p)):
        print("🚨 GTFS 필수 파일(routes/trips/shapes)이 누락되었습니다.")
        if os.path.exists(extract_path):
            shutil.rmtree(extract_path)
        return

    routes = pd.read_csv(routes_p, encoding="utf-8")
    trips  = pd.read_csv(trips_p,  encoding="utf-8")
    shapes = pd.read_csv(shapes_p, encoding="utf-8")
    
    # 공백 제거 및 문자열 타입 일치
    routes["route_id"] = routes["route_id"].astype(str).str.strip()
    trips["route_id"]  = trips["route_id"].astype(str).str.strip()
    shapes["shape_id"] = shapes["shape_id"].astype(str).str.strip()

    # shapes 좌표 정렬 순서 보정
    shapes["shape_pt_sequence"] = pd.to_numeric(shapes["shape_pt_sequence"])
    shapes = shapes.sort_values(by=["shape_id", "shape_pt_sequence"])

    # 중복 노선 제거 후 병합
    unique_shapes = trips.dropna(subset=["shape_id"]).drop_duplicates(subset=["shape_id"])
    merged = unique_shapes.merge(routes, on="route_id")

    features = []
    for _, row in merged.iterrows():
        shape_id = str(row["shape_id"]).strip()
        
        # 노선 이름 추출 (가장 긴 명칭 -> 짧은 명칭 -> ID 기본값 순서)
        r_long = row.get("route_long_name")
        r_short = row.get("route_short_name")
        route_name = str(r_long).strip() if pd.notna(r_long) and str(r_long).strip() else (str(r_short).strip() if pd.notna(r_short) else f"Line {row['route_id']}")

        # 💡 [색상 수용] route_color 속성을 파싱하되, 없으면 메트로 대표 색상(#007AFF) 부여
        raw_color = row.get("route_color")
        if pd.notna(raw_color) and str(raw_color).strip():
            color_str = str(raw_color).strip().replace("#", "")
            route_color = f"#{color_str}"
        else:
            route_color = "#007AFF"

        pts = shapes[shapes["shape_id"] == shape_id]
        
        # GeoJSON 표준 정순서 [경도(lon), 위도(lat)] 조립
        coords = []
        for _, pt in pts.iterrows():
            lon_val = float(pt["shape_pt_lon"])
            lat_val = float(pt["shape_pt_lat"])
            coords.append([lon_val, lat_val])

        if len(coords) < 2:
            continue

        features.append({
            "type": "Feature",
            "geometry": {"type": "LineString", "coordinates": coords},
            "properties": {
                "routeName": str(route_name),
                "routeColor": str(route_color)
            },
        })

    geojson = {"type": "FeatureCollection", "features": features}
    output_file = os.path.join(OUTPUT_DIR, f"{city_id}.json")
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(geojson, f, ensure_ascii=False, indent=2)

    print(f"✅ [성공] 완료! 대도시 지하철/철도망 {len(features)}개 노선 추출 완료 → {output_file}")
    shutil.rmtree(extract_path)

if __name__ == "__main__":
    process_taiwan_gtfs()
