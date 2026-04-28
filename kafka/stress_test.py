"""
SECOM Pipeline - Stress Test (부하 테스트)
==========================================
Usage:
    python kafka/stress_test.py <workers> <duration_sec> <rate_per_worker>

Examples:
    python kafka/stress_test.py 1 30  1    # baseline: 1프로세스, 30초, 초당1건
    python kafka/stress_test.py 1 60  10   # 10x:  초당10건
    python kafka/stress_test.py 2 60  50   # 100x: 2프로세스 × 초당50건
    python kafka/stress_test.py 4 60  250  # 1000x: 4프로세스 × 초당250건
    python kafka/stress_test.py 4 60  0    # max: 4프로세스, 속도제한 없음
"""

import json
import os
import sys
import time
import random
from datetime import datetime, timezone
from multiprocessing import Process, Value, Lock

import pandas as pd
from confluent_kafka import Producer


# ── 설정 ──────────────────────────────────────────────
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "secom-sensors")
CSV_PATH = os.getenv("SECOM_CSV_PATH", "data/uci-secom.csv")


def load_data():
    """CSV 로드 + 센서 컬럼 분리"""
    df = pd.read_csv(CSV_PATH)
    sensor_cols = [c for c in df.columns if c not in ("Time", "Pass/Fail")]
    return df, sensor_cols


def build_message(row, sensor_cols, worker_id, rate):
    """producer.py와 동일한 메시지 포맷"""
    return {
        "event_time": datetime.now(timezone.utc).isoformat(),
        "sensors": {
            col: (None if pd.isna(row[col]) else round(float(row[col]), 4))
            for col in sensor_cols
        },
        "pass_fail": int(row["Pass/Fail"]),
        "metadata": {
            "source": "stress_test",
            "worker_id": worker_id,
            "replay_speed": rate,
            "version": "v1",
        },
    }


def worker_run(worker_id, duration_sec, rate, shared_sent, shared_fail, lock):
    """
    개별 워커 프로세스.
    - rate > 0: 초당 rate건 전송 (sleep 기반 조절)
    - rate == 0: 속도 제한 없이 최대 속도로 전송
    """
    df, sensor_cols = load_data()
    indices = df.index.tolist()

    producer = Producer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "broker.address.family": "v4",
        "acks": "all",
        "retries": 3,
        "retry.backoff.ms": 500,
        "queue.buffering.max.messages": 100000,
        "queue.buffering.max.kbytes": 1048576,  # 1GB
        "batch.num.messages": 1000,
        "linger.ms": 50,
    })

    local_sent = 0
    local_fail = 0

    def on_delivery(err, msg):
        nonlocal local_sent, local_fail
        if err:
            local_fail += 1
        else:
            local_sent += 1

    start = time.time()
    deadline = start + duration_sec
    interval = 1.0 / rate if rate > 0 else 0

    print(f"[Worker {worker_id}] 시작 | rate={rate}/s | duration={duration_sec}s")

    idx_pos = 0
    random.shuffle(indices)

    while time.time() < deadline:
        row = df.loc[indices[idx_pos % len(indices)]]
        message = build_message(row, sensor_cols, worker_id, rate)

        try:
            producer.produce(
                TOPIC,
                value=json.dumps(message, default=str).encode("utf-8"),
                callback=on_delivery,
            )
            producer.poll(0)
        except BufferError:
            producer.poll(1.0)  # 버퍼 가득 → 콜백 소화 후 재시도
            try:
                producer.produce(
                    TOPIC,
                    value=json.dumps(message, default=str).encode("utf-8"),
                    callback=on_delivery,
                )
            except BufferError:
                local_fail += 1

        idx_pos += 1
        if idx_pos % len(indices) == 0:
            random.shuffle(indices)

        # 속도 조절
        if interval > 0:
            elapsed_in_sec = time.time() - start
            expected = idx_pos * interval
            sleep_time = expected - elapsed_in_sec
            if sleep_time > 0:
                time.sleep(sleep_time)

    # 남은 메시지 flush
    producer.flush(timeout=10)

    elapsed = time.time() - start
    throughput = local_sent / elapsed if elapsed > 0 else 0

    with lock:
        shared_sent.value += local_sent
        shared_fail.value += local_fail

    print(f"[Worker {worker_id}] 종료 | sent={local_sent} | fail={local_fail} | "
          f"{elapsed:.1f}s | {throughput:.1f} msg/s")


def main():
    if len(sys.argv) < 4:
        print(__doc__)
        sys.exit(1)

    workers = int(sys.argv[1])
    duration_sec = int(sys.argv[2])
    rate = int(sys.argv[3])

    # CSV 존재 확인
    if not os.path.exists(CSV_PATH):
        print(f"에러: {CSV_PATH} 없음")
        sys.exit(1)

    total_rate = rate * workers if rate > 0 else "unlimited"
    print("=" * 60)
    print(f"SECOM Stress Test")
    print(f"  Workers:    {workers}")
    print(f"  Duration:   {duration_sec}s")
    print(f"  Rate/worker: {rate}/s ({'unlimited' if rate == 0 else rate})")
    print(f"  Total rate:  {total_rate}/s")
    print(f"  Topic:       {TOPIC}")
    print("=" * 60)

    shared_sent = Value("i", 0)
    shared_fail = Value("i", 0)
    lock = Lock()

    start = time.time()

    processes = []
    for i in range(workers):
        p = Process(
            target=worker_run,
            args=(i, duration_sec, rate, shared_sent, shared_fail, lock),
        )
        processes.append(p)
        p.start()

    for p in processes:
        p.join()

    elapsed = time.time() - start
    total_sent = shared_sent.value
    total_fail = shared_fail.value
    throughput = total_sent / elapsed if elapsed > 0 else 0

    print("\n" + "=" * 60)
    print(f"결과 요약")
    print(f"  총 전송:     {total_sent:,}")
    print(f"  총 실패:     {total_fail:,}")
    print(f"  소요 시간:   {elapsed:.1f}s")
    print(f"  처리량:      {throughput:.1f} msg/s")
    print(f"  실패율:      {total_fail / max(total_sent + total_fail, 1) * 100:.2f}%")
    print("=" * 60)


if __name__ == "__main__":
    main()
