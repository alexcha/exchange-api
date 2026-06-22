import os
import requests
import zipfile
import pandas as pd
import json
import shutil

OUTPUT_DIR = "m/output"  # yml의 m/output/ 맞춤
os.makedirs(OUTPUT_DIR, exist_ok=True)

MOBILITY_TOKEN = os.environ["MOBILITY_TOKEN"]  # yml 시크릿명 맞춤

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

    # 1. GTFS 텍스트 파일들을 데이터프레임으로 로드
    routes = pd.read_csv(os.path.join(extract_path, "routes.txt"), encoding="utf-8")
    trips  = pd.read_csv(os.path.join(extract_path, "trips.txt"),  encoding="utf-8")
    shapes = pd.read_csv(shapes_path, encoding="utf-8")
    
    # 💡 [보정 1] 데이터 병합 누락을 막기 위해 route_id 컬럼을 확실하게 문자열 타입으로 일치시키고 공백을 제거합니다.
    routes["route_id"] = routes["route_id"].astype(str).str.strip()
    trips["route_id"]  = trips["route_id"].astype(str).str.strip()

    # 2. 정렬 및 유니크한 shape_id 매핑 추출
    shapes = shapes.sort_values(["shape_id", "shape_pt_sequence"])
    unique_shapes = trips.dropna(subset=["shape_id"]).drop_duplicates(subset=["shape_id"])
    
    # 3. 데이터프레임 병합
    merged = unique_shapes.merge(routes, on="route_id")

    features = []
    for _, row in merged.iterrows():
        shape_id = row["shape_id"]
        
        # 💡 [보정 2] 이름 데이터가 결측치(NaN)일 경우를 대비해 3단계 방어벽을 세워 문자열로 안전하게 파싱합니다.
        r_long = row.get("route_long_name")
        r_short = row.get("route_short_name")
        
        route_name = ""
        if pd.notna(r_long) and str(r_long).strip() and str(r_long).lower() != "nan":
            route_name = str(r_long).strip()
        elif pd.notna(r_short) and str(r_short).strip() and str(r_short).lower() != "nan":
            route_name = str(r_short).strip()
        else:
            route_name = f"Line {row['route_id']}"

        # 💡 [보정 3] Pandas가 빈 칸을 float 형태의 NaN으로 가져오면서 생기는 데이터 오염을 판별해 냅니다.
        color_raw = row.get("route_color")
        if pd.isna(color_raw) or not str(color_raw).strip() or str(color_raw).lower() == "nan":
            route_color = "#1b74e4"  # 노선 색상이 누락되었을 때 사용할 기본 시스템 블루
        else:
            # 혹시 기존 데이터에 # 부호가 붙어있거나 떨어져 있어도 유연하게 대처
            color_str = str(color_raw).strip().replace("#", "")
            route_color = f"#{color_str}"

        # 4. 해당 노선의 좌표 리스트 빌드
        pts    = shapes[shapes["shape_id"] == shape_id]
        coords = [[float(lon), float(lat)]
                  for lat, lon in zip(pts["shape_pt_lat"], pts["shape_pt_lon"])]
        if len(coords) < 2:
            continue

        # 💡 [보정 4] 안드로이드 MapLibre가 온전하게 수신할 수 있도록 확실하게 키와 밸류를 properties에 담아줍니다.
        features.append({
            "type": "Feature",
            "geometry": {"type": "LineString", "coordinates": coords},
            "properties": {
                "routeName": str(route_name),
                "routeColor": str(route_color)
            },
        })

    # 5. 최종 GeoJSON 저장
    geojson = {"type": "FeatureCollection", "features": features}
    output_file = os.path.join(OUTPUT_DIR, f"{city_id}.json")
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(geojson, f, ensure_ascii=False, indent=2)

    print(f"✅ 완료! {len(features)}개 노선 → {output_file}")
    shutil.rmtree(extract_path)


if __name__ == "__main__":
    process_taiwan_gtfs()
