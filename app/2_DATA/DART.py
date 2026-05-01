from datetime import date, timedelta
import io
import os
from pathlib import Path
import sqlite3
import zipfile

from dotenv import load_dotenv
from bs4 import BeautifulSoup
import pandas as pd
import requests
import streamlit as st


load_dotenv()


def fetch_dart_disclosure_list(
    end_de: str,
    api_key: str | None = None,
    page_count: int = 100,
    last_reprt_at: bool = True,
) -> pd.DataFrame:
    """Fetch DART disclosure list up to end_de in YYYYMMDD format."""
    api_key = api_key or os.getenv("DART_API_KEY")
    if not api_key:
        raise ValueError("DART_API_KEY가 설정되어 있지 않습니다.")

    url = "https://opendart.fss.or.kr/api/list.json"
    all_rows = []
    page_no = 1
    total_page = 1

    while page_no <= total_page:
        params = {
            "crtfc_key": api_key,
            "end_de": end_de,
            "page_no": page_no,
            "page_count": page_count,
            "last_reprt_at": "Y" if last_reprt_at else "N",
        }

        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()

        if data.get("status") != "000":
            message = data.get("message", "알 수 없는 오류")
            raise RuntimeError(f"DART API 오류: status={data.get('status')}, message={message}")

        total_page = int(data.get("total_page", 1))
        all_rows.extend(data.get("list", []))
        page_no += 1

    return pd.DataFrame(all_rows)


def save_dart_disclosure_list(
    df: pd.DataFrame,
    end_de: str,
    db_path: Path | None = None,
) -> Path:
    """Save fetched DART disclosures into SQLite."""
    db_path = db_path or Path("data/DART/dart.sqlite")
    init_dart_db(db_path)

    columns = [
        "rcept_no",
        "corp_code",
        "corp_name",
        "stock_code",
        "corp_cls",
        "report_nm",
        "flr_nm",
        "rcept_dt",
        "rm",
    ]
    save_df = df.reindex(columns=columns).fillna("")
    records = [
        tuple(str(row[column]).strip() for column in columns) + (end_de,)
        for _, row in save_df.iterrows()
    ]

    with sqlite3.connect(db_path) as conn:
        conn.executemany(
            """
            INSERT INTO dart_disclosures (
                rcept_no, corp_code, corp_name, stock_code, corp_cls,
                report_nm, flr_nm, rcept_dt, rm, end_de
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(rcept_no) DO UPDATE SET
                corp_code = excluded.corp_code,
                corp_name = excluded.corp_name,
                stock_code = excluded.stock_code,
                corp_cls = excluded.corp_cls,
                report_nm = excluded.report_nm,
                flr_nm = excluded.flr_nm,
                rcept_dt = excluded.rcept_dt,
                rm = excluded.rm,
                end_de = excluded.end_de
            """,
            records,
        )
    return db_path


def init_dart_db(db_path: Path | None = None) -> Path:
    """Create DART SQLite tables if they do not exist."""
    db_path = db_path or Path("data/DART/dart.sqlite")
    db_path.parent.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS dart_disclosures (
                rcept_no TEXT PRIMARY KEY,
                corp_code TEXT,
                corp_name TEXT,
                stock_code TEXT,
                corp_cls TEXT,
                report_nm TEXT,
                flr_nm TEXT,
                rcept_dt TEXT,
                rm TEXT,
                end_de TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS dart_summaries (
                rcept_no TEXT PRIMARY KEY,
                llm_summary TEXT,
                FOREIGN KEY (rcept_no) REFERENCES dart_disclosures(rcept_no)
            )
            """
        )
        conn.execute(
            """
            CREATE VIEW IF NOT EXISTS dart_summary_view AS
            SELECT
                d.rcept_no,
                d.corp_code,
                d.corp_name,
                d.stock_code,
                d.corp_cls,
                d.report_nm,
                d.flr_nm,
                d.rcept_dt,
                d.rm,
                d.end_de,
                s.llm_summary
            FROM dart_disclosures AS d
            LEFT JOIN dart_summaries AS s
                ON d.rcept_no = s.rcept_no
            """
        )
    return db_path


def extract_text_from_document_zip(zip_content: bytes) -> tuple[str, str]:
    """Extract text from the first XML file inside a DART document zip."""
    with zipfile.ZipFile(io.BytesIO(zip_content)) as zf:
        xml_names = [name for name in zf.namelist() if name.lower().endswith(".xml")]
        if not xml_names:
            raise ValueError("압축 파일 안에 XML 파일이 없습니다.")

        target_xml = xml_names[0]
        raw = zf.read(target_xml)

    text = raw.decode("utf-8")
    soup = BeautifulSoup(text, "xml")
    body = soup.find("BODY")
    body_text = "\n".join(body.stripped_strings) if body else "\n".join(soup.stripped_strings)
    return target_xml, body_text


def fetch_dart_document_text(
    rcept_no: str,
    api_key: str | None = None,
) -> tuple[str, str]:
    """Fetch a DART original document and return its XML filename and body text."""
    api_key = api_key or os.getenv("DART_API_KEY")
    if not api_key:
        raise ValueError("DART_API_KEY가 설정되어 있지 않습니다.")

    url = "https://opendart.fss.or.kr/api/document.xml"
    params = {
        "crtfc_key": api_key,
        "rcept_no": str(rcept_no),
    }

    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()

    try:
        return extract_text_from_document_zip(response.content)
    except zipfile.BadZipFile as exc:
        error_text = response.content.decode("utf-8", errors="replace")
        soup = BeautifulSoup(error_text, "xml")
        status = soup.find("status")
        message = soup.find("message")
        if status or message:
            raise RuntimeError(
                f"DART 문서 API 오류: status={status.get_text(strip=True) if status else '-'}, "
                f"message={message.get_text(strip=True) if message else '-'}"
            ) from exc
        raise


def clean_storage_text(text: str) -> str:
    """Remove control characters that are awkward to store or render."""
    return "".join(char for char in text if char in "\t\n\r" or ord(char) >= 32)


@st.cache_resource(show_spinner=False)
def load_summary_model():
    from mlx_lm import load

    model_name = "mlx-community/Qwen3-30B-A3B-Instruct-2507-4bit"
    return load(model_name)


def summarize_disclosure_text(body_text: str, model, tokenizer) -> str:
    from mlx_lm import generate

    input_text = f"""
다음은 공시 원문이다.
불필요한 문구(인사말, 반복 문장, 일반적 주의문구)는 제거하고,
핵심 투자정보만 남기고 짧게 요약하라.

[공시 원문]
{body_text}
"""

    messages = [
        {"role": "user", "content": input_text},
    ]

    prompt = tokenizer.apply_chat_template(
        messages,
        tokenize=False,
        add_generation_prompt=True,
    )

    response = generate(
        model,
        tokenizer,
        prompt=prompt,
        max_tokens=16384,
        verbose=False,
    )
    return clean_storage_text(response).strip()


def summarize_documents_from_sqlite(
    end_de: str,
    api_key: str | None = None,
    db_path: Path | None = None,
    progress_callback=None,
) -> Path:
    """Summarize DART documents from SQLite disclosure rows."""
    db_path = init_dart_db(db_path)

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        target_rows = conn.execute(
            """
            SELECT rcept_no
            FROM dart_disclosures
            WHERE end_de = ?
              AND TRIM(COALESCE(stock_code, '')) != ''
              AND TRIM(COALESCE(rcept_no, '')) != ''
            ORDER BY rcept_no
            """,
            (end_de,),
        ).fetchall()

    model, tokenizer = load_summary_model()
    total_count = len(target_rows)
    success_count = 0
    failed_count = 0

    with sqlite3.connect(db_path) as conn:
        for current_count, row in enumerate(target_rows, start=1):
            current_rcept_no = str(row["rcept_no"]).strip()

            if progress_callback:
                progress_callback(current_count - 1, total_count, current_rcept_no, success_count, failed_count)

            try:
                _, body_text = fetch_dart_document_text(
                    rcept_no=current_rcept_no,
                    api_key=api_key,
                )
                body_text = clean_storage_text(body_text)
                summary = summarize_disclosure_text(body_text, model, tokenizer)

                conn.execute(
                    """
                    INSERT INTO dart_summaries (rcept_no, llm_summary)
                    VALUES (?, ?)
                    ON CONFLICT(rcept_no) DO UPDATE SET
                        llm_summary = excluded.llm_summary
                    """,
                    (current_rcept_no, summary),
                )
                conn.commit()
                success_count += 1

            except Exception:
                failed_count += 1

            if progress_callback:
                progress_callback(current_count, total_count, current_rcept_no, success_count, failed_count)

    return db_path


def format_date(selected_date: date) -> str:
    return selected_date.strftime("%Y%m%d")


st.title("DART 전자공시")

st.markdown(
    """
    이 페이지는 DART OpenAPI에서 공시 목록을 수집하고, 공시 원문을 로컬 LLM으로 요약해 SQLite DB에 저장하는 과정입니다.

    1. 먼저 조회 종료일을 선택한 뒤 `공시 목록 저장`을 실행합니다. 선택한 날짜까지의 DART 공시 목록을 수집하고
       `data/DART/dart.sqlite`의 `dart_disclosures` 테이블에 저장합니다.
    2. 다음으로 `원문 요약 저장`을 실행합니다. DB에서 선택한 날짜의 공시 중 `stock_code`와 `rcept_no`가 있는
       행만 골라 각 접수번호의 원문 문서를 DART 원문 API에서 가져옵니다.
    3. DART 원문 문서는 zip 파일 형태로 내려오며, 내부 XML 파일의 `BODY` 영역을 파싱해 텍스트만 추출합니다.
    4. 추출한 원문은 로컬 MLX 모델 `Qwen3-30B-A3B-Instruct-2507-4bit`에 전달해 핵심 투자정보만 짧게 요약합니다.
    5. 요약 결과는 `dart_summaries` 테이블의 `llm_summary` 컬럼에 저장합니다. 목록과 요약을 함께 보고 싶을 때는
       `dart_summary_view`를 조회하면 됩니다.

    실행 순서는 `공시 목록 저장` 이후 `원문 요약 저장`입니다.
    """
)

api_key = os.getenv("DART_API_KEY")
if not api_key:
    st.warning(".env 파일에 DART_API_KEY를 설정해 주세요.")

selected_date = st.date_input(
    "조회 종료일",
    value=date.today() - timedelta(days=1),
    max_value=date.today(),
)

if st.button("공시 목록 저장"):
    end_de = format_date(selected_date)

    try:
        with st.spinner("공시 목록을 SQLite DB에 저장하는 중입니다."):
            df_all = fetch_dart_disclosure_list(
                end_de=end_de,
                api_key=api_key,
            )
            db_path = save_dart_disclosure_list(df_all, end_de)
        st.success(f"저장 완료: {db_path}")

    except Exception as exc:
        st.error(str(exc))

if st.button("원문 요약 저장"):
    end_de = format_date(selected_date)

    try:
        progress = st.progress(0)
        status = st.empty()

        def update_summary_progress(
            current_count: int,
            total_count: int,
            rcept_no: str,
            success_count: int,
            failed_count: int,
        ) -> None:
            progress.progress(current_count / total_count if total_count else 1.0)
            status.info(
                f"{current_count}/{total_count} 처리 중 - 접수번호 {rcept_no} "
                f"(성공 {success_count}, 실패 {failed_count})"
            )

        with st.spinner("원문을 불러와 LLM 요약을 생성하는 중입니다."):
            db_path = summarize_documents_from_sqlite(
                end_de=end_de,
                api_key=api_key,
                progress_callback=update_summary_progress,
            )
        progress.progress(1.0)
        st.success(f"저장 완료: {db_path}")

    except Exception as exc:
        st.error(str(exc))
