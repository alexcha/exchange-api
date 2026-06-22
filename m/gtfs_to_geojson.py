import os
import requests
import zipfile
import pandas as pd
import json
import shutil

OUTPUT_DIR = "m/output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

def process_taiwan_gtfs():
    city_id      = "taiwan"
    zip_path     = f"{city_id}_gtfs.zip"
    extract_path = f"{city_id}_extracted"

    # 🎯 [핵심 수정] API를 거치지 않고 타이베이 메트로(MRT) 공식 GTFS 주소 직접 타겟팅
    feed_url = "https://transitfeeds.com/p/taipei-mass-rapid-transit/841/latest/download"

    print("[1/3] 타이베이 메트로 공식 GTFS 다운로드 중...")
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    resp = requests.get(feed_url, headers=headers, stream=True, timeout=90)
    resp.raise_for_status()
    with open(zip_path, "wb") as f:
        for chunk in resp.iter_content(chunk_size=8192):
            f.write(chunk)

    print("[2/3] 압축 해제 중...")
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
        print("🚨 GTFS 필수 파일이 누락되었습니다.")
        if os.path.exists(extract_path):
            shutil.rmtree(extract_path)
        return

    print("[3/3] 타이베이 지하철 노선 GeoJSON 생성 중...")
    routes = pd.read_csv(routes_p, encoding="utf-8")
    trips  = pd.read_csv(trips_p,  encoding="utf-8")
    shapes = pd.read_csv(shapes_p, encoding="utf-8")
    
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
        
        r_long = row.get("route_long_name")
        r_short = row.get("route_short_name")
        route_name = str(r_long).strip() if pd.notna(r_long) and str(r_long).strip() else (str(r_short).strip() if pd.notna(r_short) else f"Line {row['route_id']}")

        # 노선 고유 색상 추출 (예: 단수이신이선=빨간색, 반난선=파란색)
        raw_color = row.get("route_color")
        if pd.notna(raw_color) and str(raw_color).strip():
            color_str = str(raw_color).strip().replace("#", "")
            route_color = f"#{color_str}"
        else:
            route_color = "#007AFF"

        pts = shapes[shapes["shape_id"] == shape_id]
        
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

    print(f"✅ [성공] 타이베이 메트로 {len(features)}개 노선 추출 완료 → {output_file}")
    shutil.rmtree(extract_path)

if __name__ == "__main__":
    process_taiwan_gtfs()
