import os
import requests
import zipfile
import pandas as pd
import json
import shutil

OUTPUT_DIR = "m/output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# 💡 GitHub Secrets에 등록한 '리프레시 토큰'을 읽어옵니다.
REFRESH_TOKEN = os.environ.get("MOBILITY_TOKEN")

def get_access_token():
    """
    💡 제공해주신 curl 명령어를 파이썬 코드로 구현한 구역입니다.
    리프레시 토큰을 사용해 API 호출용 억세스 토큰을 실시간 발급받습니다.
    """
    url = "https://api.mobilitydatabase.org/v1/tokens"
    headers = {"Content-Type": "application/json"}
    payload = {"refresh_token": REFRESH_TOKEN}
    
    try:
        print("🔄 Mobility Database 토큰 갱신 요청 중...")
        response = requests.post(url, headers=headers, json=payload, timeout=10)
        if response.status_code == 200:
            token_data = response.json()
            # API 응답 규격에 따라 access_token 혹은 id_token 구조를 추출합니다.
            access_token = token_data.get("access_token") or token_data.get("id_token")
            if access_token:
                print("✅ 엑세스 토큰 발급 성공!")
                return access_token
        print(f"❌ 토큰 발급 실패. 상태 코드: {response.status_code}, 응답: {response.text}")
    except Exception as e:
        print(f"❌ 토큰 요청 중 예외 발생: {e}")
    return None

def process_city_gtfs(city_id, url, access_token):
    print(f"\n[{city_id}] 작업 시작...")
    zip_path = f"{city_id}_gtfs.zip"
    extract_path = f"{city_id}_extracted"
    
    # 💡 실시간으로 발급받은 access_token을 Bearer 헤더에 주입합니다.
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json"
    }
    
    try:
        print(f"[{city_id}] GTFS 다운로드 시도 중...")
        response = requests.get(url, headers=headers, stream=True, timeout=30)
        
        if response.status_code != 200:
            print(f"[{city_id}] 다운로드 실패! HTTP 상태 코드: {response.status_code}")
            return
            
        with open(zip_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"[{city_id}] 다운로드 성공 ➡️ 압축 해제 중...")
        
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_path)
            
        print(f"[{city_id}] GeoJSON 파싱 및 변환 시작...")
        
        routes = pd.read_csv(f"{extract_path}/routes.txt", usecols=['route_id', 'route_long_name', 'route_color'])
        trips = pd.read_csv(f"{extract_path}/trips.txt", usecols=['route_id', 'trip_id', 'shape_id'])
        shapes = pd.read_csv(f"{extract_path}/shapes.txt", usecols=['shape_id', 'shape_pt_lat', 'shape_pt_lon', 'shape_pt_sequence'])
        
        shapes = shapes.sort_values(by=['shape_id', 'shape_pt_sequence'])
        
        features = []
        unique_shapes = trips.dropna(subset=['shape_id']).drop_duplicates(subset=['shape_id'])
        merged = unique_shapes.merge(routes, on='route_id')
        
        for _, row in merged.iterrows():
            shape_id = row['shape_id']
            route_name = row['route_long_name']
            route_color = f"#{row['route_color']}" if pd.notna(row['route_color']) else "#000000"
            
            shape_pts = shapes[shapes['shape_id'] == shape_id]
            coordinates = [[float(lon), float(lat)] for lat, lon in zip(shape_pts['shape_pt_lat'], shape_pts['shape_pt_lon'])]
            
            if len(coordinates) < 2:
                continue
                
            features.append({
                "type": "Feature",
                "geometry": {
                    "type": "LineString",
                    "coordinates": coordinates
                },
                "properties": {
                    "routeName": str(route_name),
                    "routeColor": route_color
                }
            })
            
        geojson = {
            "type": "FeatureCollection",
            "features": features
        }
        
        output_file = os.path.join(OUTPUT_DIR, f"{city_id}.json")
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(geojson, f, ensure_ascii=False, indent=2)
            
        print(f"[{city_id}] 변환 완료 및 저장 성공 ➡️ {output_file}")
        
    except Exception as e:
        print(f"[{city_id}] 처리 중 에러 발생: {e}")
        
    finally:
        if os.path.exists(zip_path): 
            os.remove(zip_path)
        if os.path.exists(extract_path):
            shutil.rmtree(extract_path)

if __name__ == "__main__":
    # 1. 엑세스 토큰 먼저 실시간으로 따오기
    token = get_access_token()
    
    if token:
        target_cities = {
            "seoul": "https://api.mobilitydatabase.org/v1/sources/mdb-1554/download",
        }
        
        for city, url in target_cities.items():
            process_city_gtfs(city, url, token)
    else:
        print("🚨 유효한 엑세스 토큰이 없어 전체 변환 작업을 중단합니다.")
