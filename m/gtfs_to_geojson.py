import os
import requests
import json
import shutil

OUTPUT_DIR = "m/output"

# 💡 [핵심] 기존 변수명(MOBILITY_TOKEN)을 그대로 유지하되, 내부적으로는 Transitland API 호출에 사용합니다.
TRANSITLAND_KEY = os.environ.get("MOBILITY_TOKEN")

def fetch_metro_geojson(city_id: str, search_param: dict):
    """
    Transitland API v2를 사용해 특정 도시/국가의 지하철(route_type=1) 노선을 
    가공된 GeoJSON 형태로 한 번에 긁어오는 함수입니다.
    """
    print(f"🎬 [{city_id.upper()}] Transitland API 호출 시작...")
    
    url = "https://transit.land/api/v2/public/routes"
    
    # 💡 기존에 세팅된 토큰 값을 Transitland 인증 헤더에 주입합니다.
    headers = {
        "apikey": TRANSITLAND_KEY
    }
    
    # route_type=1 (Subway / Metro / 지하철만 필터링)
    params = {
        "route_type": 1,
        "include_geometry": "true",  # 선형(LineString) 데이터 포함
        "limit": 100                 # 가져올 최대 노선 수
    }
    params.update(search_param)
    
    try:
        resp = requests.get(url, headers=headers, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        print(f"❌ [{city_id}] API 요청 중 에러 발생: {e}")
        return

    routes_list = data.get("routes", [])
    if not routes_list:
        print(f"⚠️ [{city_id}] 검색 결과에 메트로(Subway) 노선이 없습니다.")
        return

    features = []
    for route in routes_list:
        geometry = route.get("geometry")
        if not geometry or geometry.get("type") != "LineString":
            continue
            
        r_long = route.get("route_long_name")
        r_short = route.get("route_short_name")
        route_name = str(r_long).strip() if r_long else (str(r_short).strip() if r_short else f"Line {route.get('onestop_id')}")
        
        # 노선 고유 컬러 맵핑
        raw_color = route.get("route_color")
        route_color = f"#{str(raw_color).strip().replace('#', '')}" if raw_color else "#007AFF"

        features.append({
            "type": "Feature",
            "geometry": geometry,  # Transitland가 보정한 깨끗한 좌표 배열
            "properties": {
                "routeName": route_name,
                "routeColor": route_color
            }
        })

    geojson = {
        "type": "FeatureCollection",
        "features": features
    }
    
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    output_file = os.path.join(OUTPUT_DIR, f"{city_id}.json")
    
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(geojson, f, ensure_ascii=False, indent=2)
        
    print(f"✅ [{city_id.upper()}] 총 {len(features)}개 메트로 노선 생성 완료! ➔ {output_file}")


def main():
    if not TRANSITLAND_KEY:
        print("🚨 에러: MOBILITY_TOKEN 변수 환경 설정값을 읽을 수 없습니다.")
        return

    if os.path.exists(OUTPUT_DIR):
        shutil.rmtree(OUTPUT_DIR)
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # 🗺️ 원하는 글로벌 도시/국가 타겟 세팅
    global_targets = {
        "taiwan": {"country": "TW"},  # 대만 전체 지하철 (타이베이 메트로 등)
        "seoul": {"country": "KR"},   # 한국 전체 지하철 (서울 메트로 및 국철 등)
        "newyork": {"bbox": "-74.259,40.477,-73.700,40.917"} # 뉴욕 메트로
    }

    for city_id, search_param in global_targets.items():
        fetch_metro_geojson(city_id, search_param)

if __name__ == "__main__":
    main()
