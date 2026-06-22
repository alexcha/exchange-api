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
    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {access_token}",
    }
    params = {"country_code": "TW", "limit": 100, "offset": 0}
    resp = requests.get(MDB_FEEDS_URL, headers=headers, params=params, timeout=15)
    resp.raise_for_status()
    feeds = resp.json()

    for feed in feeds:
        producer_url = (feed.get("source_info") or {}).get("producer_url", "")
        if producer_url:
            print(f"  피드 선택: {feed.get('id')} / {feed.get('provider')}")
            print(f"  producer_url: {producer_url}")
            return producer_url

    raise RuntimeError("대만 피드를 찾을 수 없습니다.")


def process_taiwan_gtfs():
    city_id      = "taiwan"
    zip_path     = f"{city_id}_gtfs.zip"
    extract_path = f"{city_id}_extracted"

    print("[1/4] access token 발급 중...")
    access_token = get_access_token()

    print("[2/4] 대만 피드 검색 중...")
    feed_url = get_taiwan_feed_url(access_token)

    print("[3/4] GTFS zip 다운로드 중...")
    resp = requests.get(feed_url, stream=True, timeout=60)
    resp.raise_for_status()
    with open(zip_path, "wb") as f:
        for chunk in resp.iter_content(chunk_size=8192):
            f.write(chunk)

    print("[4/4] 압축 해제 및 GeoJSON 변환 중...")
    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            zf.extractall(extract_path)
    except zipfile.BadZipFile:
        print("❌ 유효하지 않은 ZIP 파일")
        return
    finally:
        if os.path.exists(zip_path):
            os.remove(zip_path)

    shapes_path = os.path.join(extract_path, "shapes.txt")
    if not os.path.exists(shapes_path):
        print("🚨 shapes.txt 없음")
        shutil.rmtree(extract_path)
        return

    routes = pd.read_csv(os.path.join(extract_path, "routes.txt"), encoding="utf-8")
    trips  = pd.read_csv(os.path.join(extract_path, "trips.txt"),  encoding="utf-8")
    shapes = pd.read_csv(shapes_path, encoding="utf-8")
    
    # 데이터 매핑 누락 방지 타입 통일
    routes["route_id"] = routes["route_id"].astype(str).str.strip()
    trips["route_id"]  = trips["route_id"].astype(str).str.strip()

    shapes = shapes.sort_values(["shape_id", "shape_pt_sequence"])
    unique_shapes = trips.dropna(subset=["shape_id"]).drop_duplicates(subset=["shape_id"])
    merged = unique_shapes.merge(routes, on="route_id")

    features = []
    for _, row in merged.iterrows():
        shape_id = row["shape_id"]
        
        r_long = row.get("route_long_name")
        r_short = row.get("route_short_name")
        route_name = str(r_long).strip() if pd.notna(r_long) and str(r_long).strip() else (str(r_short).strip() if pd.notna(r_short) else f"Line {row['route_id']}")

        # 💡 대만 routes.txt 데이터에 route_color 컬럼이 아예 없으므로 눈에 띄는 선명한 블루(#007AFF)를 기본색으로 완전 고정 주입합니다.
        route_color = "#007AFF"

        pts = shapes[shapes["shape_id"] == shape_id]
        
        # 💡 [핵심 버그 수정] zip 매핑 순서 오염을 완벽히 방지하여 표준 GeoJSON 규격인 [경도, 위도] 정순서로 조립합니다.
        coords = []
        for _, pt in pts.iterrows():
            lon_val = float(pt["shape_pt_lon"]) # 경도 (120.xx)
            lat_val = float(pt["shape_pt_lat"]) # 위도 (24.xx)
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

    print(f"✅ 완료! {len(features)}개 노선 → {output_file}")
    shutil.rmtree(extract_path)


if __name__ == "__main__":
    process_taiwan_gtfs()
