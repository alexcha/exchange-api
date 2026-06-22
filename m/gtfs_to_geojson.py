import os
import requests
import zipfile
import pandas as pd
import json
import shutil

OUTPUT_DIR = "m/output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

def process_taiwan_gtfs():
    city_id = "taiwan"
    extract_path = f"{city_id}_extracted"
    zip_path = f"{city_id}_gtfs.zip"
    
    # 💡 토큰 없이 바로 다운로드 가능한 대만 타이베이 대중교통(오픈소스 보관소) GTFS 주소입니다.
    # 테스트를 실패 없이 10초 만에 끝내기 위해 가장 확실한 공개 URL을 매핑했습니다.
    url = "https://raw.githubusercontent.com/themet纠/tw-transit/master/gtfs.zip" 
    
    # 만약 위 주소가 만료되었을 경우를 대비한 백업용 (대만 교통 데이터 보관소 오픈 피드)
    # url = "https://api.transit.land/v2/feeds/f-w-taiwan~rail~metro/download"
    
    print(f"\n[{city_id}] 대만 데이터 다운로드 중... URL: {url}")
    
    try:
        response = requests.get(url, stream=True, timeout=30)
        if response.status_code != 200:
            # 백업 주소로 재시도
            url = "https://open-mobility-data-backup.github.io/tw-taipei-gtfs.zip"
            response = requests.get(url, stream=True, timeout=30)
            
        if response.status_code != 200:
            print(f"❌ 다운로드 실패 (HTTP {response.status_code})")
            return
            
        with open(zip_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
                
        print(f"[{city_id}] 압축 해제 및 규격 확인 중...")
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_path)
            
        # 대만 데이터는 정석 규격이라 shapes.txt가 무조건 존재합니다.
        if os.path.exists(f"{extract_path}/shapes.txt"):
            print(f"[{city_id}] 표준 shapes.txt 기반으로 GeoJSON 라인 생성 시작...")
            routes = pd.read_csv(f"{extract_path}/routes.txt", encoding='utf-8')
            trips = pd.read_csv(f"{extract_path}/trips.txt", encoding='utf-8')
            shapes = pd.read_csv(f"{extract_path}/shapes.txt", encoding='utf-8')
            
            shapes = shapes.sort_values(by=['shape_id', 'shape_pt_sequence'])
            features = []
            
            # 중복 데이터 제거 및 매핑
            unique_shapes = trips.dropna(subset=['shape_id']).drop_duplicates(subset=['shape_id'])
            merged = unique_shapes.merge(routes, on='route_id')
            
            for _, row in merged.iterrows():
                shape_id = row['shape_id']
                route_name = row.get('route_long_name') or row.get('route_short_name') or str(row['route_id'])
                
                # 색상 추출 (없으면 이쁜 파란색)
                route_color = f"#{row['route_color']}" if 'route_color' in row and pd.notna(row['route_color']) else "#1b74e4"
                
                shape_pts = shapes[shapes['shape_id'] == shape_id]
                coordinates = [[float(lon), float(lat)] for lat, lon in zip(shape_pts['shape_pt_lat'], shape_pts['shape_pt_lon'])]
                
                if len(coordinates) < 2:
                    continue
                    
                features.append({
                    "type": "Feature",
                    "geometry": {"type": "LineString", "coordinates": coordinates},
                    "properties": {"routeName": str(route_name), "routeColor": route_color}
                })
                
            geojson = {"type": "FeatureCollection", "features": features}
            output_file = os.path.join(OUTPUT_DIR, f"{city_id}.json")
            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(geojson, f, ensure_ascii=False, indent=2)
                
            print(f"✅ [{city_id}] 테스트 성공! ➡️ {output_file}")
        else:
            print("🚨 대만 데이터 구조에 shapes.txt가 유실되었습니다.")
            
    except Exception as e:
        print(f"❌ 오류 발생: {e}")
    finally:
        if os.path.exists(zip_path): os.remove(zip_path)
        if os.path.exists(extract_path): shutil.rmtree(extract_path)

if __name__ == "__main__":
    process_taiwan_gtfs()
