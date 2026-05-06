import streamlit as st
import requests
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import json

# ── 설정 ──────────────────────────────────────────────
API_URL = "http://secom-api:8000"

st.set_page_config(
    page_title="SECOM 이상 탐지 대시보드",
    page_icon="🏭",
    layout="wide",
)


def api_get(endpoint):
    """FastAPI GET 요청"""
    try:
        res = requests.get(f"{API_URL}{endpoint}", timeout=5)
        res.raise_for_status()
        return res.json()
    except Exception as e:
        st.error(f"API 연결 실패: {e}")
        return None


def api_post(endpoint, data):
    """FastAPI POST 요청"""
    try:
        res = requests.post(f"{API_URL}{endpoint}", json=data, timeout=5)
        res.raise_for_status()
        return res.json()
    except Exception as e:
        st.error(f"API 연결 실패: {e}")
        return None


# ── 사이드바: 페이지 선택 ──────────────────────────────
st.sidebar.title("🏭 SECOM Pipeline")
page = st.sidebar.radio(
    "페이지 선택",
    ["📊 대시보드", "🔍 이상치 모니터링", "📈 일일 집계", "🤖 모델 예측"],
)

# ── 사이드바: 시스템 상태 ──────────────────────────────
st.sidebar.markdown("---")
st.sidebar.subheader("시스템 상태")
health = api_get("/health")
if health and health.get("status") == "healthy":
    st.sidebar.success("✅ API 정상")
else:
    st.sidebar.error("❌ API 연결 실패")


# ================================================================
# 페이지 1: 대시보드 (요약)
# ================================================================
if page == "📊 대시보드":
    st.title("📊 SECOM 파이프라인 대시보드")

    # 상단 지표 카드
    col1, col2, col3, col4 = st.columns(4)

    # raw_events 건수
    raw = api_get("/raw-events/count")
    if raw:
        col1.metric("총 이벤트", f"{raw['total_count']:,}")
        col2.metric("최근 이벤트", raw.get("latest_event_kst", "-")[:19] if raw.get("latest_event_kst") else "-")

    # anomalies 통계
    stats = api_get("/anomalies/stats")
    if stats:
        col3.metric("총 이상치", f"{stats['total_anomalies']:,}")
        col4.metric("총 스코어링", f"{stats.get('total_scored', 0):,}")

    st.markdown("---")

    # 유형별 이상치 분포
    if stats and stats.get("by_type"):
        st.subheader("🔴 유형별 이상치 분포")
        df_type = pd.DataFrame(stats["by_type"])
        df_top10 = df_type.nlargest(10, "count")

        fig = px.bar(
            df_top10,
            x="anomaly_type",
            y="count",
            color="avg_anomaly_score",
            color_continuous_scale="Reds",
            labels={"anomaly_type": "탐지 유형", "count": "이상치 건수", "avg_anomaly_score": "평균 이상 점수"},
            title="탐지 유형별 이상치 발생 건수",
        )
        fig.update_layout(xaxis_type="category")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("아직 이상치 데이터가 없습니다. Spark Streaming을 실행해주세요.")


# ================================================================
# 페이지 2: 이상치 모니터링
# ================================================================
elif page == "🔍 이상치 모니터링":
    st.title("🔍 실시간 이상치 모니터링")

    # 새로고침 버튼
    if st.button("🔄 새로고침"):
        st.rerun()

    # 최근 이상치 목록
    limit = st.slider("조회 건수", 10, 100, 20)
    data = api_get(f"/anomalies/latest?limit={limit}")

    if data and data.get("anomalies"):
        st.metric("조회된 이상치", f"{data['count']}건")

        df = pd.DataFrame(data["anomalies"])
        df = df.rename(columns={
            "event_time_kst": "시간 (KST)",
            "anomaly_score": "Anomaly Score",
            "anomaly_type": "유형",
            "model_version": "모델 버전",
        })

        # Anomaly Score 분포 차트
        st.subheader("Anomaly Score 분포")
        fig = px.histogram(
            df, x="Anomaly Score", nbins=20,
            color_discrete_sequence=["#e74c3c"],
            title="이상치 점수 분포",
        )
        st.plotly_chart(fig, use_container_width=True)

        # 테이블
        st.subheader("이상치 상세")
        st.dataframe(
            df[["시간 (KST)", "Anomaly Score", "유형", "모델 버전"]],
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.info("이상치 데이터가 없습니다.")


# ================================================================
# 페이지 3: 일일 집계
# ================================================================
elif page == "📈 일일 집계":
    st.title("📈 일일 집계 트렌드")

    data = api_get("/daily-agg")

    if data and data.get("data"):
        df = pd.DataFrame(data["data"])
        df["date"] = pd.to_datetime(df["date"])
        df = df.sort_values("date")

        # anomaly_rate를 퍼센트로 변환 (0.3588 → 35.88)
        df["anomaly_rate_pct"] = df["anomaly_rate"] * 100

        # 이벤트 수 + 이상치 수 트렌드
        st.subheader("일별 이벤트 및 이상치")
        fig = go.Figure()
        fig.add_trace(go.Bar(x=df["date"], y=df["total_events"], name="총 이벤트", marker_color="#3498db"))
        fig.add_trace(go.Bar(x=df["date"], y=df["anomaly_count"], name="이상치", marker_color="#e74c3c"))
        fig.update_layout(barmode="overlay", xaxis_title="날짜", yaxis_title="건수")
        st.plotly_chart(fig, use_container_width=True)

        # 이상치 비율 트렌드 (퍼센트, Y축 0~100 고정)
        st.subheader("일별 이상치 비율 (%)")
        fig2 = px.line(
            df, x="date", y="anomaly_rate_pct",
            markers=True,
            color_discrete_sequence=["#e74c3c"],
            labels={"date": "날짜", "anomaly_rate_pct": "이상치 비율 (%)"},
        )
        fig2.add_hline(y=50, line_dash="dash", line_color="red", annotation_text="CRITICAL (50%)")
        fig2.update_layout(yaxis_range=[0, 100])
        st.plotly_chart(fig2, use_container_width=True)

        # 테이블
        st.subheader("집계 데이터")
        st.dataframe(df, use_container_width=True, hide_index=True)
    else:
        st.info("집계 데이터가 없습니다. Airflow DAG를 실행해주세요.")


# ================================================================
# 페이지 4: 모델 예측
# ================================================================
elif page == "🤖 모델 예측":
    st.title("🤖 Isolation Forest 이상 탐지 예측")
    st.caption("센서값을 입력하면 Isolation Forest 모델이 이상 여부를 판단합니다.")

    st.subheader("센서값 입력")
    col1, col2, col3, col4, col5 = st.columns(5)
    s0 = col1.number_input("센서 0", value=3045.0, format="%.1f")
    s1 = col2.number_input("센서 1", value=2443.8, format="%.1f")
    s2 = col3.number_input("센서 2", value=2198.5, format="%.1f")
    s5 = col4.number_input("센서 5", value=99.8, format="%.1f")
    s10 = col5.number_input("센서 10", value=0.012, format="%.4f")

    st.caption("* 590개 센서 중 5개만 입력. 나머지는 학습 데이터 평균값으로 자동 대체됩니다.")

    if st.button("🔮 예측 실행", type="primary"):
        sensors = {"0": s0, "1": s1, "2": s2, "5": s5, "10": s10}
        result = api_post("/predict", {"sensors": sensors})

        if result:
            st.markdown("---")

            # 게이지 차트 (Gemini 피드백 반영)
            score = result["anomaly_score"]
            is_anomaly = result["is_anomaly"]

            col_result, col_gauge = st.columns([1, 2])

            with col_result:
                if is_anomaly:
                    st.error("🚨 ANOMALY DETECTED")
                else:
                    st.success("✅ Normal")
                st.metric("Anomaly Score", f"{score:.4f}")
                st.caption("score가 낮을수록(음수) 이상 가능성 높음")

            with col_gauge:
                fig = go.Figure(go.Indicator(
                    mode="gauge+number",
                    value=score,
                    domain={"x": [0, 1], "y": [0, 1]},
                    title={"text": "Anomaly Score"},
                    gauge={
                        "axis": {"range": [-0.5, 0.5]},
                        "bar": {"color": "#e74c3c" if is_anomaly else "#2ecc71"},
                        "steps": [
                            {"range": [-0.5, -0.1], "color": "#fadbd8"},
                            {"range": [-0.1, 0.1], "color": "#fef9e7"},
                            {"range": [0.1, 0.5], "color": "#d5f5e3"},
                        ],
                        "threshold": {
                            "line": {"color": "red", "width": 4},
                            "thickness": 0.75,
                            "value": 0,
                        },
                    },
                ))
                fig.update_layout(height=300)
                st.plotly_chart(fig, use_container_width=True)
