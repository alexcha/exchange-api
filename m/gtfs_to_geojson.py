
import os
import requests
import zipfile
import pandas as pd
import json

os.makedirs("metro_data", exist_ok=True)

# 💡 깃허브 액션 환경변수에서 토큰을 읽어옵니다.
API_TOKEN = os.environ.get("MOBILITY_TOKEN")

def download_gtfs_with_auth(url, save_path):
    # 헤더에 Bearer 토큰을 실어서 다운로드 권한 획득
    headers = {
        "Authorization": f"Bearer {API_TOKEN}",
        "Accept": "application/json"
    }
    
    print(f"인증 헤더와 함께 GTFS 다운로드 시도 중...")
    response = requests.get(url, headers=headers, stream=True)
    
    if response.status_code == 200:
        with open(save_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        print("다운로드 성공!")
        return True
    else:
        print(f"다운로드 실패 코드: {response.status_code}")
        return False

# 이후 전개 방식(압축 해제 및 GeoJSON 파싱 로직)은 이전과 동일하게 처리됩니다!
