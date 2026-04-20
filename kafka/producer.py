import json
import time
import random
import sys
import signal
from datetime import datetime

import pandas as pd
from confluent_kafka import Producer


# 1. 설정값
BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "secom-sensors"
REPLAY_SPEED = int(sys.argv[1]) if len(sys.argv) > 1 else 1
CSV_PATH = "data/uci-secom.csv"

# 2. SECOM CSV 로드
try:
    df = pd.read_csv(CSV_PATH)
except FileNotFoundError:
    print(f"에러: {CSV_PATH} 파일을 찾을 수 없습니다")
    print("https://www.kaggle.com/datasets/paresh2047/uci-semcom 에서 다운로드 받으세요")
    sys.exit(1)

sensor_cols = [col for col in df.columns if col not in ["Time", "Pass/Fail"]]
print(f"Loaded {len(df)} rows, {len(sensor_cols)} sensors")
print(f"Replay speed: {REPLAY_SPEED}x ({REPLAY_SPEED} messages/sec)")
print(f"Topic: {TOPIC}")

# 3. Kafka Producer 생성
try:
    producer = Producer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "broker.address.family": "v4",
        "acks": "all",
        "retries": 3,
        "retry.backoff.ms": 500,
    })
except Exception as e:
    print(f"에러: 프로듀서 생성 실패: {e}")
    sys.exit(1)

# 4. 전송 카운터
sent_count = 0
fail_count = 0

def delivery_callback(err, msg):
    global sent_count, fail_count
    if err:
        fail_count += 1
    else:
        sent_count += 1

# 5. 메시지 생성 함수
def build_message(row):
    return {
        "event_time": datetime.now().isoformat(),
        "sensors": {
            col: (None if pd.isna(row[col]) else round(float(row[col]), 4))
            for col in sensor_cols
        },
        "pass_fail": int(row["Pass/Fail"]),
        "metadata": {
            "source": "secom_replay",
            "replay_speed": REPLAY_SPEED,
            "version": "v1",
        },
    }

# 6. 무한 순환 발행
print(f"\n발행 시작... (Ctrl+C 로 중지)\n")
try:
    cycle = 0
    while True:
        cycle += 1
        indices = df.index.tolist()
        random.shuffle(indices)

        for i, idx in enumerate(indices):
            row = df.loc[idx]
            message = build_message(row)

            label = "FAIL" if message["pass_fail"] == 1 else "PASS"

            try:
                producer.produce(
                    TOPIC,
                    value=json.dumps(message, default=str).encode("utf-8"),
                    callback=delivery_callback,
                )
                producer.poll(0)
            except BufferError:
                print("경고 : 프로듀서 버퍼 가득 참, 대기 중...")
                producer.flush()
                producer.produce(
                    TOPIC,
                    value=json.dumps(message, default=str).encode("utf-8"),
                    callback=delivery_callback,
                )

            print(f"[Cycle {cycle}] {i+1}/{len(indices)} | {label} | sensor_0={message['sensors'].get('0', 'N/A')} | {message['event_time']}")

            time.sleep(1 / REPLAY_SPEED)

except KeyboardInterrupt:
    pass
finally:
    print(f"\n\n남은 메시지 전송 중...")
    producer.flush()
    producer.close()
    print(f"Total sent: {sent_count} | Total failed: {fail_count}")
    print(f"Cycles completed: {cycle if 'cycle' in dir() else 0}")
