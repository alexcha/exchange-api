import os
import requests
import zipfile
import pandas as pd
import json
import shutil

# 💡 결과물 폴더 경로를 m/output 으로 정확히 지정합니다.
OUTPUT_DIR = "m/output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# 💡 깃허브 액션 환경변수에서 토큰을 안전하게 읽어옵니다.
API_TOKEN = os.environ.get("MOBILITY_TOKEN")

def process_city_gtfs(city_id, url):
    print(f"[{city_id}] 작업 시작...")
    zip_path = f"{city_id}_gtfs.zip"
    extract_path = f"{city_id}_extracted"
    
    # 헤더에 Bearer 토큰을 실어서 다운로드 권한 획득
    headers = {
        "Authorization": f"Bearer {API_TOKEN}",
        "Accept": "application/json"
    }
    
    try:
        print(f"[{city_id}] 인증 헤더와 함께 GTFS 다운로드 시도 중... URL: {url}")
        response = requests.get(url, headers=headers, stream=True)
        
        if response.status_code != 200:
            print(f"[{city_id}] 다운로드 실패! HTTP 상태 코드: {response.status_code}")
            return
            
        with open(zip_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"[{city_id}] 다운로드 성공 ➡️ 압축 해제 중...")
        
        # 압축 해제
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_path)
            
        print(f"[{city_id}] GeoJSON 파싱 및 변환 시작...")
        
        # 메모리 방어를 위해 필요한 컬럼만 지정해서 CSV 읽기
        routes = pd.read_csv(f"{extract_path}/routes.txt", usecols=['route_id', 'route_long_name', 'route_color'])
        trips = pd.read_csv(f"{extract_path}/trips.txt", usecols=['route_id', 'trip_id', 'shape_id'])
        shapes = pd.read_csv(f"{extract_path}/shapes.txt", usecols=['shape_id', 'shape_pt_lat', 'shape_pt_lon', 'shape_pt_sequence'])
        
        # 기하학적 선형 정렬
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
        
        # 최종 가공된 GeoJSON을 m/output/{city_id}.json 에 저장
        output_file = os.path.join(OUTPUT_DIR, f"{city_id}.json")
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(geojson, f, ensure_ascii=False, indent=2)
            
        print(f"[{city_id}] 변환 완료 및 저장 성공 ➡️ {output_file}")
        
    except Exception as e:
        print(f"[{city_id}] 처리 중 에러 발생: {e}")
        
    finally:
        # 임시 파일 및 폴더 삭제 청소
        if os.path.exists(zip_path): 
            os.remove(zip_path)
        if os.path.exists(extract_path):
            shutil.rmtree(extract_path)

if __name__ == "__main__":
    # 💡 [반영 완료] 다운로드하고자 하는 도시 피드 목록 정의
    # 새로운 도시를 추가하고 싶을 때는 아래 딕셔너리에 한 줄씩 주석을 풀거나 주소를 추가해 주면 됩니다.
    target_cities = {
        "seoul": "https://api.mobilitydatabase.org/v1/sources/mdb-1554/download", # 서울/수도권 정식 피드
        # "tokyo": "https://api.mobilitydatabase.org/v1/sources/mdb-1051/download",
        # "newyork": "https://api.mobilitydatabase.org/v1/sources/mdb-1234/download",
    }
    
    for city, url in target_cities.items():
        process_city_gtfs(city, url)
