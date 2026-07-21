import json
import os
from datetime import datetime, timezone

import firebase_admin
from firebase_admin import credentials, messaging


def init_firebase():
    """fetch_and_enrich_gdacs.py의 init_firebase()와 동일한 로직(독립 스크립트라 중복 보유)."""
    if not firebase_admin._apps:
        cred_json_str = os.environ.get('FIREBASE_SERVICE_ACCOUNT_JSON')
        if not cred_json_str:
            print("⚠️  [FCM] FIREBASE_SERVICE_ACCOUNT_JSON 환경변수가 없어서 발송을 건너뜁니다.")
            return False
        try:
            cred_json = json.loads(cred_json_str)
            cred = credentials.Certificate(cred_json)
            firebase_admin.initialize_app(cred)
            return True
        except Exception as e:
            print(f"❌ [FCM] 파이어베이스 라이브러리 초기화 중 오류: {e}")
            return False
    return True


def send_travel_risk_sync_ping():
    """
    ⭐️ [신규] 재난 이벤트 없이 "국가 위험도(travel_risk.json)만" 바뀐 경우를 위한 전용 신호.

    배경: 클라이언트가 앱 내부 1시간 폴링을 제거하고, FCM data 메시지를 받았을 때만
    TravelRiskApi.triggerFullSync()로 위험도+재난 데이터를 함께 재동기화하는 구조로
    바뀌었다. 그런데 재난 알림(send_disaster_push)은 GDACS 재난 이벤트가 신규/갱신
    됐을 때만 발송되므로 — "재난은 없고 국가 위험도 단계만 바뀐" 경우엔 지금까지
    FCM이 전혀 발송되지 않아, 클라이언트 지도가 구버전 위험도로 멈춰있는 문제가 있었다.

    이 함수는 travel_risk.json이 실제로 변경됐을 때(워크플로에서 git diff로 판별)
    딱 한 번, 알림(Notification)은 전혀 만들지 않고 "데이터만 다시 받아라"는
    신호만 담은 data-only 메시지를 topic "all"로 보낸다.
    클라이언트는 "type": "travel_risk_sync" 를 받으면 TravelRiskApi를 재동기화하고
    (포그라운드면) 화면만 갱신할 뿐, 사용자에게 보이는 알림은 절대 띄우지 않는다.
    (안드로이드 MyFirebaseMessagingService.handleTravelRiskSyncPush() 참고)
    """
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    try:
        message = messaging.Message(
            data={
                "type": "travel_risk_sync",
                "triggered_at": now_iso,
            },
            android=messaging.AndroidConfig(
                priority="high"
            ),
            topic="all",
        )
        response = messaging.send(message)
        print(f"  👉 [FCM 위험도 동기화 신호 발송 성공] (전송 ID: {response})")
        return True
    except Exception as e:
        print(f"  ❌ [FCM 위험도 동기화 신호 발송 실패] 에러 내용: {e}")
        return False


def main():
    if not init_firebase():
        return
    send_travel_risk_sync_ping()


if __name__ == "__main__":
    main()
