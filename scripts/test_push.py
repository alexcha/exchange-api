import os
import json
import firebase_admin
from firebase_admin import credentials, messaging

def send_test_push():
    print("🚀 Firebase FCM 테스트 푸시 시작...")
    
    # 1. GitHub Secrets에서 환경변수로 넘겨준 서비스 계정 키 로드
    # (기존 스크립트가 세팅된 방식에 따라 os.environ에서 가져옵니다)
    cred_json = os.environ.get("FIREBASE_SERVICE_ACCOUNT") 
    if not cred_json:
        print("❌ 에러: FIREBASE_SERVICE_ACCOUNT 환경변수가 없습니다.")
        return
        
    cred_dict = json.loads(cred_json)
    cred = credentials.Certificate(cred_dict)
    
    if not firebase_admin._apps:
        firebase_admin.initialize_app(cred)
        
    # 2. 전체 구독자용 토픽(예: 'all' 또는 기존에 사용 중인 토픽명) 설정
    # 만약 특정 기기 토큰으로 테스트하려면 token="YOUR_DEVICE_TOKEN" 사용
    topic_name = "all" 
    
    message = messaging.Message(
        notification=messaging.Notification(
            title="🔔 시스템 테스트 알림",
            body="GDACS 파이프라인 푸시 알림 정상 작동 테스트입니다.",
        ),
        topic=topic_name,
    )
    
    # 3. 발송
    response = messaging.send(message)
    print(Successfully sent message: {response}")

if __name__ == "__main__":
    send_test_push()
