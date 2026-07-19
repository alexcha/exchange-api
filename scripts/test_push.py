import os
import json
import datetime
import firebase_admin
from firebase_admin import credentials, messaging

def send_test_push():
    print("🚀 Firebase FCM 멕시코 테스트 푸시 시작...")
    
    # 1. GitHub Secrets에서 서비스 계정 키 로드
    cred_json = os.environ.get("FIREBASE_SERVICE_ACCOUNT") 
    if not cred_json:
        print("❌ 에러: FIREBASE_SERVICE_ACCOUNT 환경변수가 없습니다.")
        return
        
    cred_dict = json.loads(cred_json)
    cred = credentials.Certificate(cred_dict)
    
    if not firebase_admin._apps:
        firebase_admin.initialize_app(cred)
        
    topic_name = "all" 
    
    # 현재 시간 생성 (앱의 날짜 파싱 형식인 ISO 8601 형태로 지정)
    current_time = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    
    # ⭐️ 핵심 변경 사항: notification 대신 앱이 원하는 규격의 'data' 페이로드로 전송
    message = messaging.Message(
        data={
            "iso_code": "MX",               # 👈 안드로이드 앱이 체크할 멕시코 국가 코드
            "last_updated": current_time,   # 👈 앱 내 formatUpdateTime()에서 파싱할 시간
            "id": "test_mexico_999",        # 고유 이벤트 ID (알림 중복 방지용 문자열)
            "country": "Mexico"
        },
        topic=topic_name,
    )
    
    # 3. 발송
    response = messaging.send(message)
    print(f"✅ Successfully sent message: {response}") # 문법 오류 수정 완료

if __name__ == "__main__":
    send_test_push()
