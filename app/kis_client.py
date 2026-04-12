import os
from datetime import datetime

import requests
import streamlit as st
from dotenv import load_dotenv

load_dotenv()


def _get_required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing environment variable: {name}")
    return value


def get_kis_config() -> dict:
    app_key = _get_required_env("KIS_APP_KEY")
    app_secret = _get_required_env("KIS_APP_SECRET")
    account_no = _get_required_env("KIS_ACCOUNT_NO")
    account_cd = _get_required_env("KIS_ACCOUNT_CD")
    is_paper = os.getenv("KIS_IS_PAPER", "true").lower() == "true"

    base_url = (
        "https://openapivts.koreainvestment.com:29443"
        if is_paper
        else "https://openapi.koreainvestment.com:9443"
    )
    ws_url = (
        "ws://ops.koreainvestment.com:31000"
        if is_paper
        else "ws://ops.koreainvestment.com:21000"
    )

    return {
        "app_key": app_key,
        "app_secret": app_secret,
        "account_no": account_no,
        "account_cd": account_cd,
        "is_paper": is_paper,
        "base_url": base_url,
        "ws_url": ws_url,
    }


def _request_access_token(config: dict) -> dict:
    url = f"{config['base_url']}/oauth2/tokenP"
    headers = {"content-type": "application/json; charset=utf-8"}
    body = {
        "grant_type": "client_credentials",
        "appkey": config["app_key"],
        "appsecret": config["app_secret"],
    }
    res = requests.post(url, headers=headers, json=body, timeout=10)
    return res.json()


def _request_websocket_approval_key(config: dict) -> dict:
    url = f"{config['base_url']}/oauth2/Approval"
    headers = {"content-type": "application/json; charset=utf-8"}
    body = {
        "grant_type": "client_credentials",
        "appkey": config["app_key"],
        "secretkey": config["app_secret"],
    }
    res = requests.post(url, headers=headers, json=body, timeout=10)
    return res.json()


def ensure_kis_session(force_refresh: bool = False) -> dict:
    if "kis_session" in st.session_state and not force_refresh:
        return st.session_state.kis_session

    config = get_kis_config()
    token_data = _request_access_token(config)
    approval_data = _request_websocket_approval_key(config)

    access_token = token_data.get("access_token", "")
    approval_key = approval_data.get("approval_key", "")

    session_data = {
        "config": config,
        "access_token": access_token,
        "approval_key": approval_key,
        "token_data": token_data,
        "approval_data": approval_data,
        "loaded_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "token_ok": bool(access_token),
        "approval_ok": bool(approval_key),
    }
    st.session_state.kis_session = session_data
    return session_data


def _build_auth_headers(config: dict, access_token: str, tr_id: str, tr_cont: str = "") -> dict:
    return {
        "content-type": "application/json; charset=utf-8",
        "authorization": f"Bearer {access_token}",
        "appkey": config["app_key"],
        "appsecret": config["app_secret"],
        "tr_id": tr_id,
        "custtype": "P",
        "tr_cont": tr_cont,
    }


def get_account_asset_status(access_token: str, config: dict) -> dict:
    url = f"{config['base_url']}/uapi/domestic-stock/v1/trading/inquire-account-balance"
    headers = _build_auth_headers(config=config, access_token=access_token, tr_id="CTRP6548R")
    params = {
        "CANO": config["account_no"],
        "ACNT_PRDT_CD": config["account_cd"],
        "INQR_DVSN_1": "",
        "BSPR_BF_DT_APLY_YN": "",
    }
    res = requests.get(url, headers=headers, params=params, timeout=10)
    return res.json()


def get_stock_balance(
    access_token: str,
    config: dict,
    ctx_area_fk100: str = "",
    ctx_area_nk100: str = "",
) -> dict:
    url = f"{config['base_url']}/uapi/domestic-stock/v1/trading/inquire-balance"
    tr_id = "VTTC8434R" if config["is_paper"] else "TTTC8434R"
    headers = _build_auth_headers(config=config, access_token=access_token, tr_id=tr_id)
    params = {
        "CANO": config["account_no"],
        "ACNT_PRDT_CD": config["account_cd"],
        "AFHR_FLPR_YN": "N",
        "OFL_YN": "",
        "INQR_DVSN": "01",
        "UNPR_DVSN": "01",
        "FUND_STTL_ICLD_YN": "N",
        "FNCG_AMT_AUTO_RDPT_YN": "N",
        "PRCS_DVSN": "01",
        "CTX_AREA_FK100": ctx_area_fk100,
        "CTX_AREA_NK100": ctx_area_nk100,
    }
    res = requests.get(url, headers=headers, params=params, timeout=10)
    return res.json()
