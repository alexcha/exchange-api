import os
import json
import datetime
import firebase_admin
from firebase_admin import credentials, messaging

def send_mexico_test_push():
    print("🚀 안드로이드 코드 매칭 - 멕시코 푸시 발송 시작...")
    
    # 1. 인증 및 초기화
    cred_json = os.environ.get("FIREBASE_SERVICE_ACCOUNT") 
    if not cred_json:
        print("❌ 에러: FIREBASE_SERVICE_ACCOUNT 환경변수가 없습니다.")
        return
        
    cred_dict = json.loads(cred_json)
    cred = credentials.Certificate(cred_dict)
    
    if not firebase_admin._apps:
        firebase_admin.initialize_app(cred)
        
    topic_name = "all" 
    
    # 2. 안드로이드 parseAsUtc()가 인식할 수 있는 깔끔한 ISO 8601 UTC 시간 생성
    # 예: 2026-07-19T18:30:00Z
    current_utc_time = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    
    # 3. ⭐️ 올려주신 자바 코드 규격에 맞춘 'data' 페이로드 구성
    message = messaging.Message(
        data={
            "iso_code": "MX",                  # resolveEventIso2() 에서 파싱
            "last_updated": current_utc_time,  # onMessageReceived() 및 formatUpdateTime() 에서 파싱
            "id": "mexico_test_event_777",     # eventKey 생성 및 알림 ID(hashCode)용
            "country": "Mexico"                # 백업용 국가명 필드
        },
        topic=topic_name,
    )
    
    # 4. 발송
    response = messaging.send(message)
    print(f"✅ 발송 성공! Message ID: {response}")

if __name__ == "__main__":
    send_mexico_test_push()
