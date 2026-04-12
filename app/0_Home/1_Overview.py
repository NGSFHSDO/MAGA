import pandas as pd
import streamlit as st

from app.kis_client import (
    ensure_kis_session,
    get_account_asset_status,
    get_stock_balance,
)


st.set_page_config(page_title="AI Hedge Fund", page_icon="📈")


def _to_number(value):
    return pd.to_numeric(value, errors="coerce")


def _fmt(value, digits: int = 0) -> str:
    number = _to_number(value)
    if pd.isna(number):
        return "-"
    if digits > 0:
        return f"{number:,.{digits}f}"
    return f"{number:,.0f}"


def _pick_first_non_empty(source: dict, keys: list[str]):
    for key in keys:
        value = source.get(key)
        if value not in (None, "", "0", "0.0", "0.00", "0.00000000"):
            return value
    for key in keys:
        if key in source:
            return source.get(key)
    return None


def _build_holdings_df(balance_data: dict) -> pd.DataFrame:
    rows = balance_data.get("output1", [])
    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    rename_map = {
        "pdno": "종목코드",
        "prdt_name": "종목명",
        "hldg_qty": "보유수량",
        "ord_psbl_qty": "주문가능수량",
        "pchs_avg_pric": "매입평균가",
        "prpr": "현재가",
        "evlu_amt": "평가금액",
        "evlu_pfls_amt": "평가손익",
        "evlu_pfls_rt": "수익률(%)",
    }
    for old, new in rename_map.items():
        if old in df.columns:
            df = df.rename(columns={old: new})

    display_cols = [col for col in rename_map.values() if col in df.columns]
    if display_cols:
        df = df[display_cols].copy()

    for col in ["보유수량", "주문가능수량", "매입평균가", "현재가", "평가금액", "평가손익", "수익률(%)"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


st.title("_AI Hedge Fund_")
st.subheader("Account Overview")

try:
    kis_session = ensure_kis_session()
except Exception as e:
    st.error(f"KIS 설정/인증 초기화 실패: {e}")
    st.stop()

config = kis_session["config"]
access_token = kis_session["access_token"]
approval_key = kis_session["approval_key"]

with st.expander("KIS Session Status", expanded=True):
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Mode", "Paper" if config["is_paper"] else "Live")
    c2.metric("Token", "OK" if kis_session["token_ok"] else "FAILED")
    c3.metric("WebSocket Key", "OK" if kis_session["approval_ok"] else "FAILED")
    c4.metric("Loaded At", kis_session["loaded_at"])
    st.caption(f"BASE_URL: {config['base_url']}")
    st.caption(f"WS_URL: {config['ws_url']}")
    st.caption(f"Account: {config['account_no']}-{config['account_cd']}")

if not access_token:
    st.error(f"Access token 발급 실패: {kis_session['token_data']}")
    st.stop()

if not approval_key:
    st.warning(f"WebSocket approval key 발급 실패: {kis_session['approval_data']}")

account_asset_data = get_account_asset_status(access_token=access_token, config=config)
balance_data = get_stock_balance(access_token=access_token, config=config)

asset_ok = account_asset_data.get("rt_cd") == "0"
balance_ok = balance_data.get("rt_cd") == "0"

if not asset_ok:
    st.error(f"투자계좌자산현황조회 실패: {account_asset_data}")
if not balance_ok:
    st.error(f"주식잔고조회 실패: {balance_data}")

asset_output2 = account_asset_data.get("output2", {}) if asset_ok else {}
balance_output2_rows = balance_data.get("output2", []) if balance_ok else []
balance_output2 = balance_output2_rows[0] if balance_output2_rows else {}
holdings = balance_data.get("output1", []) if balance_ok else []

total_asset = _pick_first_non_empty(asset_output2, ["tot_asst_amt", "nass_tot_amt"])
total_eval = _pick_first_non_empty(asset_output2, ["evlu_amt_smtl", "evlu_amt_smtl_amt"])
total_pnl = _pick_first_non_empty(asset_output2, ["evlu_pfls_amt_smtl", "evlu_pfls_smtl_amt"])
cash_like = _pick_first_non_empty(balance_output2, ["dnca_tot_amt", "nass_amt"])

c1, c2, c3, c4 = st.columns(4)
c1.metric("총자산", _fmt(total_asset))
c2.metric("총평가금액", _fmt(total_eval))
c3.metric("평가손익합계", _fmt(total_pnl))
c4.metric("예수금/순자산", _fmt(cash_like))

c5, c6, c7, c8 = st.columns(4)
c5.metric("보유 종목 수", str(len(holdings)))
c6.metric("D+1 출금가능", _fmt(balance_output2.get("nxdy_excc_amt")))
c7.metric("D+2 자동상환", _fmt(balance_output2.get("d2_auto_rdpt_amt")))
c8.metric("자산증감률(%)", _fmt(balance_output2.get("asst_icdc_erng_rt"), digits=2))

holdings_df = _build_holdings_df(balance_data)
st.markdown("### 보유 종목")
if holdings_df.empty:
    st.info("보유 종목이 없습니다. (output1 empty)")
else:
    st.dataframe(holdings_df, use_container_width=True, hide_index=True)

st.markdown("### Raw JSON")
with st.expander("6. 투자계좌자산현황조회 - Raw Output", expanded=False):
    st.json(account_asset_data)
with st.expander("7. 주식잔고조회 - Raw Output", expanded=False):
    st.json(balance_data)
