# Kafka 수집 설계

## 1. 데이터 소스

- 데이터셋: SECOM (UCI ML Repository / Kaggle)
- 반도체 공정 센서 590개 + Pass/Fail 라벨
- 1567 샘플, 2008년 7월~10월 수집
- 양품 1463 / 불량 104

## 2. Producer 코드 흐름

1. pandas로 SECOM CSV 로드 (1567행)
2. 인덱스 리스트 랜덤 셔플
3. for문으로 한 행씩 순회:
  1. 센서 590개 값을 딕셔너리 값으로 변환 (NaN은 None 처리)
  2. event_time을 현재 시간으로 덮어쓰기
  3. pass_fail, metadata 포함한 JSON 메시지 생성
  4. confluent  kafka Producer로 Kafka 토픽에 발행
  5. callback으로 성공/실패 확인
  6. REPLAY SPEED에 따라 sleep (1/REPLAY_SPEED초)
4. 1567행 끝나면 다시 셔플 후 무한 반복
5. Ctrl+C 시 flush 후 종료

## 3. 데이터 수집 로직

- 방식: CSV Replay (정적 데이터를 실시간 스트림으로 시뮬레이션)
- 순서: 매 순환마다 랜덤 셔플 (같은 순서 반복 방지)
- 시간: 원본 timestamp 무시, 발행 시점의 현재 시간 사용
- 속도: 명령줄 인자로 조절 (python producer.py 10 = 10배속)

## 4. 메시지 생성 방식

- 포맷: JSON
- 직렬화: json.dumps() -> .encode("utf-8") -> bytes
- NaN 처리: pandas.isna() 확인 후 None 변환 (JSON null)
- 타입 변환: numpy float -> Python float, numpy int -> Python int
- 소수점: round(value, 4)로 부동소수점 정리

## 5. 메시지 예시

```json
{
  "event_time": "2026-04-19T16:30:00.123456",
  "sensors": {
    "0": 3030.93,
    "1": 2564.0,
    "2": 2187.7333,
    "3": 1411.1265,
    .
    .
    .
    "589": null
  },
  "pass_fail": -1,
  "metadata": {
    "source": "secom_replay",
    "replay_speed": 1,
    "version": "v1"
  }
}
```

필드 설명:

- event_time: 발행 시점 현재 시간
- sensors: 590개 센서값 (key: 센서 번호, value: 측정값 또는 null)
- pass_fail: -1 (양품) 또는 +1 (불량)
- metadata.source: 데이터 출처 식별
- metadata.replay_speed: 현재 재생 속도
- metadata.version: 메시지 포맷 버전 (향후 변경 추적용)

## 6. Error Handling 전략

### 유실 방지

- acks=all 설정으로 broker가 저장 확인할 때까지 대기하여 유실 방지
- retries=3으로 네트워크 등 일시 장애 시 자동 재시도
- retry.backoff.ms=500으로 재시도 간격 0.5초

### 중복 처리

- exactly-once는 설정 복잡도 대비 효용이 낮아 미적용
- acks=all + retries 조합으로 at-least-once 보장
- 중복 발생 시 Spark consumer에서 event_time 기준 중복제거 처리 예정

### 버퍼 초과

- replay 속도를 1000x까지 올리면 Producer 버퍼가 가득 찰 수 있음
- BufferError 발생 시 flush 로 버퍼 비운 후 재시도

### 장애 감지

- delivery callback으로 매 메시지 성공/실패 추적
- 종료 시 총 sent/failed 카운트 출력하여 문제 여부 확인

## 7. Topic 구성

### Topic 이름 및 개수

- 이름: secom-sensors
- 개수: 1개
- 역할: SECOM 센서 데이터 수집 전용, Spark Streaming이 구독하여 처리

### Partitioning

- 파티션 수: 3개
- Partition Key: 미사용 (라운드로빈 방식으로 균등 분배)

### Partition Key를 안 쓴 이유

이상치 탐지가 열 단위 판정이라 파티션 간 순서가 깨져도 문제 없음. 균등 분산으로 Spark consumer 3개 병렬 처리가 처리량이 유리.

### 파티션 3개인 이유

Spark executor 3개와 매칭하여 병렬 소비. 1개는 병렬 불가, 10개는 단일 노드 환경에서 과도. 파티션은 추가는 가능하지만 축소는 불가하므로 보수적으로 시작.

## 8. Configuration 설정

### Producer 설정


| 설정                    | 값              | 이유            |
| --------------------- | -------------- | ------------- |
| bootstrap.servers     | localhost:9092 | 로컬 개발 환경      |
| broker.address.family | v4             | IPv6 연결 실패 방지 |
| acks                  | all            | 메시지 유실 방지     |
| retries               | 3              | 일시 장애 자동 복구   |
| retry.backoff.ms      | 500            | 재시도 간격        |


### Topic 설정


| 설정                 | 값         | 이유                       |
| ------------------ | --------- | ------------------------ |
| partitions         | 3         | Spark consumer 병렬도       |
| replication.factor | 1         | 단일 브로커 환경                |
| retention.ms       | 604800000 | 7일 보관                    |
| max.message.bytes  | 1048576   | 1MB (SECOM 메시지는 수KB라 충분) |


### Broker 설정 (docker-compose.yml)


| 설정                   | 값                                      | 이유               |
| -------------------- | -------------------------------------- | ---------------- |
| KAFKA_PROCESS_ROLES  | broker,controller                      | KRaft 모드         |
| KAFKA_LISTENERS      | PLAINTEXT + INTERNAL + CONTROLLER      | 외부/내부/컨트롤러 분리    |
| ADVERTISED_LISTENERS | localhost:9092 (외부) + kafka:29092 (내부) | Docker 내외부 접근 분리 |


