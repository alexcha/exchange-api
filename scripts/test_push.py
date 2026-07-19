import os
import json
import datetime
import firebase_admin
from firebase_admin import credentials, messaging

def send_perfect_mexico_push():
    print("🚀 [안드로이드 전용 규격화] 멕시코 FCM 데이터 테스트 푸시 발송...")
    
    # 1. 깃허브 시크릿 키 로드
    cred_json = os.environ.get("FIREBASE_SERVICE_ACCOUNT") 
    if not cred_json:
        print("❌ 에러: FIREBASE_SERVICE_ACCOUNT 환경변수가 비어있습니다.")
        return
        
    cred_dict = json.loads(cred_json)
    cred = credentials.Certificate(cred_dict)
    
    if not firebase_admin._apps:
        firebase_admin.initialize_app(cred)
        
    topic_name = "all" 
    
    # 2. 안드로이드 parseAsUtc() 형식을 철저히 지키는 시간값 생성
    current_utc_time = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    
    # 3. 데이터 페이로드 구성 (매칭 실패 방지를 위해 변수 후보군 폭탄 주입)
    message = messaging.Message(
        data={
            "iso_code": "MX",                  # resolveEventIso2 의 1순위 파싱 필드
            "isoCode": "MX",                   # resolveEventIso2 의 2순위 파싱 필드
            "iso3": "MEX",                     # resolveEventIso2 의 3순위 파싱 필드
            "last_updated": current_utc_time,  # 캘린더 시간 파싱용 필드
            "event_date": current_utc_time,    # 백업용 시간 필드
            "id": "mexico_final_test_1001",    # 고유 식별 해시 키 생성용
            "country": "Mexico"                # 백업용 국가 이름 필드
        },
        android=messaging.AndroidConfig(
            priority="high", # 👈 중요: 앱이 꺼져 있어도 OS 배터리 제한을 뚫고 즉시 깨움
            notification=messaging.AndroidNotification(
                channel_id="favorite_risk_alarm_channel" # 앱 채널 ID 강제 지정
            )
        ),
        topic=topic_name,
    )
    
    # 4. 발송 실행
    response = messaging.send(message)
    print(f"✅ FCM 테스트 발송 성공! 발송 코드 ID: {response}")

if __name__ == "__main__":
    send_perfect_mexico_push()
