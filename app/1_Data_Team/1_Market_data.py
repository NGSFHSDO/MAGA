import streamlit as st
import os
import requests
import pandas as pd
import plotly.graph_objects as go
from dotenv import load_dotenv
from datetime import datetime, timedelta

st.set_page_config(
    page_title="Market Data",
    page_icon="📈",
)

load_dotenv()

APP_KEY = os.getenv("KIS_APP_KEY")
APP_SECRET = os.getenv("KIS_APP_SECRET")
IS_PAPER = os.getenv("KIS_IS_PAPER", "true").lower() == "true"
BASE_URL = (
    "https://openapivts.koreainvestment.com:29443"
    if IS_PAPER
    else "https://openapi.koreainvestment.com:9443"
)

def get_access_token():
    url = f"{BASE_URL}/oauth2/tokenP"
    headers = {"content-type": "application/json"}
    body = {
        "grant_type": "client_credentials",
        "appkey": APP_KEY,
        "appsecret": APP_SECRET,
    }
    res = requests.post(url, headers=headers, json=body, timeout=10)
    data = res.json()

    if "access_token" not in data:
        st.error(f"토큰 발급 실패: {data}")
        st.stop()

    return data["access_token"]


def get_period_chart(
    access_token: str,
    stock_code: str,
    start_date: str,
    end_date: str,
    period_code: str = "D",
    org_adj_prc: str = "0",
    market_code: str = "J",
):
    url = f"{BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice"
    headers = {
        "content-type": "application/json; charset=utf-8",
        "authorization": f"Bearer {access_token}",
        "appkey": APP_KEY,
        "appsecret": APP_SECRET,
        "tr_id": "FHKST03010100",
        "custtype": "P",
    }
    params = {
        "FID_COND_MRKT_DIV_CODE": market_code,
        "FID_INPUT_ISCD": stock_code,
        "FID_INPUT_DATE_1": start_date,
        "FID_INPUT_DATE_2": end_date,
        "FID_PERIOD_DIV_CODE": period_code,
        "FID_ORG_ADJ_PRC": org_adj_prc,
    }
    res = requests.get(url, headers=headers, params=params, timeout=10)
    return res.json()


def parse_ohlcv_from_period_chart(period_chart_data: dict) -> pd.DataFrame:
    rows = period_chart_data.get("output2", [])

    if not rows:
        return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])

    df = pd.DataFrame(rows)

    # 컬럼 매핑: KIS 응답 -> OHLCV
    df = df.rename(
        columns={
            "stck_bsop_date": "date",
            "stck_oprc": "open",
            "stck_hgpr": "high",
            "stck_lwpr": "low",
            "stck_clpr": "close",
            "acml_vol": "volume",
        }
    )

    # 필요한 컬럼만 남기기
    df = df[["date", "open", "high", "low", "close", "volume"]].copy()

    # 타입 변환
    df["date"] = pd.to_datetime(df["date"], format="%Y%m%d", errors="coerce")
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # 정렬(오름차순) + 결측 제거
    df = df.sort_values("date").dropna().reset_index(drop=True)
    return df


def _to_number(value):
    return pd.to_numeric(value, errors="coerce")


def _fmt_number(value, decimals: int = 0) -> str:
    number = _to_number(value)
    if pd.isna(number):
        return "-"
    if decimals > 0:
        return f"{number:,.{decimals}f}"
    return f"{number:,.0f}"


def render_output1_summary(period_chart_data: dict):
    output1 = period_chart_data.get("output1", {})
    if not output1:
        st.info("output1 데이터가 없습니다.")
        return

    stock_name = output1.get("hts_kor_isnm", "-")
    stock_code = output1.get("stck_shrn_iscd", "-")
    st.subheader(f"{stock_name} ({stock_code})")

    current_price = _to_number(output1.get("stck_prpr"))
    change_value = _to_number(output1.get("prdy_vrss"))
    change_rate = _to_number(output1.get("prdy_ctrt"))

    delta_text = "-"
    if not pd.isna(change_value) and not pd.isna(change_rate):
        delta_text = f"{change_value:,.0f} ({change_rate:.2f}%)"

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("현재가", _fmt_number(current_price), delta_text)
    c2.metric("시가", _fmt_number(output1.get("stck_oprc")))
    c3.metric("고가", _fmt_number(output1.get("stck_hgpr")))
    c4.metric("저가", _fmt_number(output1.get("stck_lwpr")))

    c5, c6, c7, c8 = st.columns(4)
    c5.metric("누적 거래량", _fmt_number(output1.get("acml_vol")))
    c6.metric("누적 거래대금", _fmt_number(output1.get("acml_tr_pbmn")))
    c7.metric("매도호가", _fmt_number(output1.get("askp")))
    c8.metric("매수호가", _fmt_number(output1.get("bidp")))

    st.caption(
        "PER: "
        + _fmt_number(output1.get("per"), 2)
        + " | PBR: "
        + _fmt_number(output1.get("pbr"), 2)
        + " | EPS: "
        + _fmt_number(output1.get("eps"), 2)
        + " | 시가총액(HTS): "
        + _fmt_number(output1.get("hts_avls"))
    )

    detail_rows = [
        {"항목": "전일 종가", "값": _fmt_number(output1.get("stck_prdy_clpr"))},
        {"항목": "전일 시가", "값": _fmt_number(output1.get("stck_prdy_oprc"))},
        {"항목": "전일 고가", "값": _fmt_number(output1.get("stck_prdy_hgpr"))},
        {"항목": "전일 저가", "값": _fmt_number(output1.get("stck_prdy_lwpr"))},
        {"항목": "상한가", "값": _fmt_number(output1.get("stck_mxpr"))},
        {"항목": "하한가", "값": _fmt_number(output1.get("stck_llam"))},
        {"항목": "전일 거래량", "값": _fmt_number(output1.get("prdy_vol"))},
        {"항목": "전일대비 거래량", "값": _fmt_number(output1.get("prdy_vrss_vol"))},
        {"항목": "거래량 회전율", "값": _fmt_number(output1.get("vol_tnrt"), 2)},
    ]
    st.dataframe(pd.DataFrame(detail_rows), use_container_width=True, hide_index=True)


def plot_ohlcv_candlestick(df_ohlcv: pd.DataFrame):
    fig = go.Figure()

    fig.add_trace(
        go.Candlestick(
            x=df_ohlcv["date"],
            open=df_ohlcv["open"],
            high=df_ohlcv["high"],
            low=df_ohlcv["low"],
            close=df_ohlcv["close"],
            name="OHLC",
        )
    )

    fig.add_trace(
        go.Bar(
            x=df_ohlcv["date"],
            y=df_ohlcv["volume"],
            name="Volume",
            yaxis="y2",
            opacity=0.35,
        )
    )

    fig.update_layout(
        title="OHLCV Chart",
        xaxis_title="Date",
        yaxis_title="Price",
        yaxis2=dict(
            title="Volume",
            overlaying="y",
            side="right",
            showgrid=False,
        ),
        xaxis_rangeslider_visible=False,
        legend=dict(orientation="h"),
        height=700,
    )

    return fig


if "access_token" not in st.session_state:
    st.session_state.access_token = get_access_token()

st.title("_Market Data_")
ticker_code = st.text_input("Write the ticker code of the stock you are interested in.")

period_code = st.selectbox("Period", ["D", "W", "M", "Y"], index=0)

if ticker_code:
    end_date = datetime.now().strftime("%Y%m%d")
    start_date = (datetime.now() - timedelta(days=180)).strftime("%Y%m%d")

    period_chart_data = get_period_chart(
        access_token=st.session_state.access_token,
        stock_code=ticker_code,
        start_date=start_date,
        end_date=end_date,
        period_code=period_code,
    )

    if period_chart_data.get("rt_cd") != "0":
        st.error(f"API 호출 실패: {period_chart_data.get('msg1', period_chart_data)}")
        st.stop()

    render_output1_summary(period_chart_data)

    df_ohlcv = parse_ohlcv_from_period_chart(period_chart_data)

    if df_ohlcv.empty:
        st.warning("output2(OHLCV) 데이터가 없습니다.")
    else:
        fig = plot_ohlcv_candlestick(df_ohlcv)
        st.plotly_chart(fig, use_container_width=True)
