from datetime import date, datetime, timedelta
from pathlib import Path
import re
import sqlite3
import time
from urllib.parse import parse_qs, urljoin, urlparse

from bs4 import BeautifulSoup
import requests
import streamlit as st


BASE_URL = "https://finance.naver.com"
LIST_URL = "https://finance.naver.com/research/company_list.naver"
INDUSTRY_LIST_URL = "https://finance.naver.com/research/industry_list.naver"
PDF_BASE_DIR = Path("data/NFR/company_reports")
INDUSTRY_PDF_BASE_DIR = Path("data/NFR/industry_reports")


def get_headers(referer: str = LIST_URL) -> dict[str, str]:
    return {
        "User-Agent": "Mozilla/5.0",
        "Referer": referer,
    }


def get_nfr_db_path(target_date: str) -> Path:
    file_date = target_date.replace("-", "")
    return Path("data/NFR") / f"nfr_{file_date}.sqlite"


def init_nfr_db(db_path: Path) -> Path:
    db_path.parent.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS nfr_company_reports (
                nid TEXT PRIMARY KEY,
                target_date TEXT,
                company_name TEXT,
                report_title TEXT,
                broker TEXT,
                report_date TEXT,
                views INTEGER,
                target_price INTEGER,
                investment_opinion TEXT,
                body_text TEXT,
                pdf_url TEXT,
                pdf_path TEXT,
                detail_url TEXT,
                collected_at TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS nfr_industry_reports (
                nid TEXT PRIMARY KEY,
                target_date TEXT,
                industry_name TEXT,
                report_title TEXT,
                broker TEXT,
                report_date TEXT,
                views INTEGER,
                body_text TEXT,
                pdf_url TEXT,
                pdf_path TEXT,
                detail_url TEXT,
                collected_at TEXT
            )
            """
        )
    return db_path


def format_naver_date(selected_date: date) -> str:
    return selected_date.strftime("%Y-%m-%d")


def get_list_params(target_date: str, page: int) -> dict[str, str | int]:
    return {
        "keyword": "",
        "brokerCode": "",
        "searchType": "writeDate",
        "writeFromDate": target_date,
        "writeToDate": target_date,
        "itemName": "",
        "itemCode": "",
        "x": 0,
        "y": 0,
        "page": page,
    }


def get_industry_list_params(target_date: str, page: int) -> dict[str, str | int]:
    return {
        "keyword": "",
        "brokerCode": "",
        "searchType": "writeDate",
        "writeFromDate": target_date,
        "writeToDate": target_date,
        "upjong": "",
        "x": 48,
        "y": 27,
        "page": page,
    }


def fetch_list_soup(target_date: str, page: int) -> BeautifulSoup:
    response = requests.get(
        LIST_URL,
        params=get_list_params(target_date, page),
        headers=get_headers(),
        timeout=10,
    )
    response.raise_for_status()
    response.encoding = "euc-kr"
    return BeautifulSoup(response.text, "html.parser")


def fetch_industry_list_soup(target_date: str, page: int) -> BeautifulSoup:
    response = requests.get(
        INDUSTRY_LIST_URL,
        params=get_industry_list_params(target_date, page),
        headers=get_headers(INDUSTRY_LIST_URL),
        timeout=10,
    )
    response.raise_for_status()
    response.encoding = "euc-kr"
    return BeautifulSoup(response.text, "html.parser")


def get_last_page(soup: BeautifulSoup) -> int:
    for anchor in soup.select("a[href]"):
        if "맨뒤" not in anchor.get_text(" ", strip=True):
            continue

        href = anchor.get("href", "")
        query = parse_qs(urlparse(href).query)
        page_values = query.get("page", [])
        if page_values:
            return int(page_values[0])

    return 1


def extract_nid(detail_url: str) -> str:
    query = parse_qs(urlparse(detail_url).query)
    nid_values = query.get("nid", [])
    return nid_values[0] if nid_values else detail_url


def collect_report_links(target_date: str) -> list[dict[str, str]]:
    first_soup = fetch_list_soup(target_date, 1)
    last_page = get_last_page(first_soup)

    reports = []
    seen_nids = set()

    for page in range(1, last_page + 1):
        page_soup = first_soup if page == 1 else fetch_list_soup(target_date, page)

        for anchor in page_soup.select('a[href*="company_read.naver"]'):
            href = anchor.get("href")
            if not href:
                continue

            detail_url = urljoin(f"{BASE_URL}/research/", href)
            nid = extract_nid(detail_url)
            if nid in seen_nids:
                continue

            seen_nids.add(nid)
            reports.append(
                {
                    "nid": nid,
                    "list_title": anchor.get_text(" ", strip=True),
                    "detail_url": detail_url,
                }
            )

    return reports


def collect_industry_report_links(target_date: str) -> list[dict[str, str]]:
    first_soup = fetch_industry_list_soup(target_date, 1)
    last_page = get_last_page(first_soup)

    reports = []
    seen_nids = set()

    for page in range(1, last_page + 1):
        page_soup = first_soup if page == 1 else fetch_industry_list_soup(target_date, page)

        for anchor in page_soup.select('a[href*="industry_read.naver"]'):
            href = anchor.get("href")
            if not href:
                continue

            detail_url = urljoin(f"{BASE_URL}/research/", href)
            nid = extract_nid(detail_url)
            if nid in seen_nids:
                continue

            seen_nids.add(nid)
            reports.append(
                {
                    "nid": nid,
                    "list_title": anchor.get_text(" ", strip=True),
                    "detail_url": detail_url,
                }
            )

    return reports


def parse_int(text: str | None) -> int | None:
    if not text:
        return None

    numbers = re.sub(r"[^0-9]", "", str(text))
    return int(numbers) if numbers else None


def clean_filename(text: str | None) -> str:
    text = "" if text is None else str(text)
    text = re.sub(r'[\\/:*?"<>|]', "_", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text or "unknown"


def extract_report_detail(
    report: dict[str, str],
    target_date: str,
    download_pdf: bool = True,
) -> dict[str, object]:
    detail_url = report["detail_url"]
    detail_response = requests.get(
        detail_url,
        headers=get_headers(detail_url),
        timeout=10,
    )
    detail_response.raise_for_status()
    detail_response.encoding = "euc-kr"

    detail_soup = BeautifulSoup(detail_response.text, "html.parser")
    target_table = detail_soup.select_one('table.type_1[summary="종목분석 리포트 본문내용"]')
    if target_table is None:
        raise ValueError("종목분석 리포트 본문 테이블을 찾지 못했습니다.")

    subject_element = target_table.select_one("th.view_sbj")
    company_name = None
    report_title = report.get("list_title")
    broker = None
    report_date = None
    views = None

    if subject_element is not None:
        company_tag = subject_element.select_one("span em")
        if company_tag is not None:
            company_name = company_tag.get_text(strip=True)

        source_tag = subject_element.select_one("p.source")
        if source_tag is not None:
            source_text = source_tag.get_text("|", strip=True)
            source_parts = [part.strip() for part in source_text.split("|") if part.strip()]

            if len(source_parts) >= 1:
                broker = source_parts[0]
            if len(source_parts) >= 2:
                report_date = source_parts[1]
            if len(source_parts) >= 3:
                views = parse_int(source_parts[2])

        subject_copy = BeautifulSoup(str(subject_element), "html.parser")
        subject_copy_element = subject_copy.select_one("th.view_sbj")
        if subject_copy_element is not None:
            for tag in subject_copy_element.select("span, p.source"):
                tag.decompose()
            parsed_title = subject_copy_element.get_text(" ", strip=True)
            if parsed_title:
                report_title = parsed_title

    target_price_tag = target_table.select_one("em.money strong")
    target_price = parse_int(target_price_tag.get_text(strip=True) if target_price_tag else None)

    opinion_tag = target_table.select_one("em.coment")
    investment_opinion = opinion_tag.get_text(strip=True) if opinion_tag else None

    body_element = target_table.select_one("td.view_cnt")
    body_div = body_element.find("div") if body_element is not None else None
    body_text = body_div.get_text("\n", strip=True) if body_div is not None else None

    pdf_urls = []
    for anchor in target_table.select('a[href*=".pdf"]'):
        href = anchor.get("href")
        if href and href not in pdf_urls:
            pdf_urls.append(href)

    pdf_url = pdf_urls[0] if pdf_urls else None
    pdf_path = None

    if download_pdf and pdf_url:
        save_dir = PDF_BASE_DIR / target_date
        save_dir.mkdir(parents=True, exist_ok=True)

        safe_report_date = clean_filename(report_date or target_date)
        safe_company = clean_filename(company_name)
        safe_broker = clean_filename(broker)
        pdf_path = save_dir / f"{safe_report_date}_{safe_company}_{safe_broker}_{report['nid']}.pdf"

        pdf_response = requests.get(
            pdf_url,
            headers=get_headers(detail_url),
            timeout=30,
        )
        pdf_response.raise_for_status()
        pdf_path.write_bytes(pdf_response.content)

    return {
        "nid": report["nid"],
        "target_date": target_date,
        "company_name": company_name,
        "report_title": report_title,
        "broker": broker,
        "report_date": report_date,
        "views": views,
        "target_price": target_price,
        "investment_opinion": investment_opinion,
        "body_text": body_text,
        "pdf_url": pdf_url,
        "pdf_path": str(pdf_path) if pdf_path is not None else None,
        "detail_url": detail_url,
        "collected_at": datetime.now().isoformat(timespec="seconds"),
    }


def extract_industry_report_detail(
    report: dict[str, str],
    target_date: str,
    download_pdf: bool = True,
) -> dict[str, object]:
    detail_url = report["detail_url"]
    detail_response = requests.get(
        detail_url,
        headers=get_headers(detail_url),
        timeout=10,
    )
    detail_response.raise_for_status()
    detail_response.encoding = "euc-kr"

    detail_soup = BeautifulSoup(detail_response.text, "html.parser")
    target_table = detail_soup.select_one('table.type_1[summary="산업분석 리포트 본문내용"]')
    if target_table is None:
        raise ValueError("산업분석 리포트 본문 테이블을 찾지 못했습니다.")

    subject_element = target_table.select_one("th.view_sbj")
    industry_name = None
    report_title = report.get("list_title")
    broker = None
    report_date = None
    views = None

    if subject_element is not None:
        industry_tag = subject_element.select_one("span em")
        if industry_tag is not None:
            industry_name = industry_tag.get_text(strip=True)

        source_tag = subject_element.select_one("p.source")
        if source_tag is not None:
            source_text = source_tag.get_text("|", strip=True)
            source_parts = [part.strip() for part in source_text.split("|") if part.strip()]

            if len(source_parts) >= 1:
                broker = source_parts[0]
            if len(source_parts) >= 2:
                report_date = source_parts[1]
            if len(source_parts) >= 3:
                views = parse_int(source_parts[2])

        subject_copy = BeautifulSoup(str(subject_element), "html.parser")
        subject_copy_element = subject_copy.select_one("th.view_sbj")
        if subject_copy_element is not None:
            for tag in subject_copy_element.select("span, p.source"):
                tag.decompose()
            parsed_title = subject_copy_element.get_text(" ", strip=True)
            if parsed_title:
                report_title = parsed_title

    body_element = target_table.select_one("td.view_cnt")
    body_div = body_element.find("div") if body_element is not None else None
    body_text = body_div.get_text("\n", strip=True) if body_div is not None else None

    pdf_urls = []
    for anchor in target_table.select('a[href*=".pdf"]'):
        href = anchor.get("href")
        if href and href not in pdf_urls:
            pdf_urls.append(href)

    pdf_url = pdf_urls[0] if pdf_urls else None
    pdf_path = None

    if download_pdf and pdf_url:
        save_dir = INDUSTRY_PDF_BASE_DIR / target_date
        save_dir.mkdir(parents=True, exist_ok=True)

        safe_report_date = clean_filename(report_date or target_date)
        safe_industry = clean_filename(industry_name)
        safe_broker = clean_filename(broker)
        pdf_path = save_dir / f"{safe_report_date}_{safe_industry}_{safe_broker}_{report['nid']}.pdf"

        pdf_response = requests.get(
            pdf_url,
            headers=get_headers(detail_url),
            timeout=30,
        )
        pdf_response.raise_for_status()
        pdf_path.write_bytes(pdf_response.content)

    return {
        "nid": report["nid"],
        "target_date": target_date,
        "industry_name": industry_name,
        "report_title": report_title,
        "broker": broker,
        "report_date": report_date,
        "views": views,
        "body_text": body_text,
        "pdf_url": pdf_url,
        "pdf_path": str(pdf_path) if pdf_path is not None else None,
        "detail_url": detail_url,
        "collected_at": datetime.now().isoformat(timespec="seconds"),
    }


def save_report_to_sqlite(report_data: dict[str, object], db_path: Path) -> None:
    init_nfr_db(db_path)

    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO nfr_company_reports (
                nid, target_date, company_name, report_title, broker, report_date,
                views, target_price, investment_opinion, body_text, pdf_url,
                pdf_path, detail_url, collected_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(nid) DO UPDATE SET
                target_date = excluded.target_date,
                company_name = excluded.company_name,
                report_title = excluded.report_title,
                broker = excluded.broker,
                report_date = excluded.report_date,
                views = excluded.views,
                target_price = excluded.target_price,
                investment_opinion = excluded.investment_opinion,
                body_text = excluded.body_text,
                pdf_url = excluded.pdf_url,
                pdf_path = excluded.pdf_path,
                detail_url = excluded.detail_url,
                collected_at = excluded.collected_at
            """,
            (
                report_data["nid"],
                report_data["target_date"],
                report_data["company_name"],
                report_data["report_title"],
                report_data["broker"],
                report_data["report_date"],
                report_data["views"],
                report_data["target_price"],
                report_data["investment_opinion"],
                report_data["body_text"],
                report_data["pdf_url"],
                report_data["pdf_path"],
                report_data["detail_url"],
                report_data["collected_at"],
            ),
        )


def save_industry_report_to_sqlite(report_data: dict[str, object], db_path: Path) -> None:
    init_nfr_db(db_path)

    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO nfr_industry_reports (
                nid, target_date, industry_name, report_title, broker,
                report_date, views, body_text, pdf_url, pdf_path,
                detail_url, collected_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(nid) DO UPDATE SET
                target_date = excluded.target_date,
                industry_name = excluded.industry_name,
                report_title = excluded.report_title,
                broker = excluded.broker,
                report_date = excluded.report_date,
                views = excluded.views,
                body_text = excluded.body_text,
                pdf_url = excluded.pdf_url,
                pdf_path = excluded.pdf_path,
                detail_url = excluded.detail_url,
                collected_at = excluded.collected_at
            """,
            (
                report_data["nid"],
                report_data["target_date"],
                report_data["industry_name"],
                report_data["report_title"],
                report_data["broker"],
                report_data["report_date"],
                report_data["views"],
                report_data["body_text"],
                report_data["pdf_url"],
                report_data["pdf_path"],
                report_data["detail_url"],
                report_data["collected_at"],
            ),
        )


def collect_reports_for_date(
    target_date: str,
    download_pdf: bool = True,
    progress_callback=None,
) -> tuple[Path, int, int]:
    db_path = init_nfr_db(get_nfr_db_path(target_date))
    reports = collect_report_links(target_date)
    total_count = len(reports)
    success_count = 0
    failed_count = 0

    for current_count, report in enumerate(reports, start=1):
        if progress_callback:
            progress_callback(current_count - 1, total_count, report, success_count, failed_count)

        try:
            report_data = extract_report_detail(
                report=report,
                target_date=target_date,
                download_pdf=download_pdf,
            )
            save_report_to_sqlite(report_data, db_path)
            success_count += 1
        except Exception:
            failed_count += 1

        if progress_callback:
            progress_callback(current_count, total_count, report, success_count, failed_count)

        time.sleep(0.1)

    return db_path, success_count, failed_count


def collect_industry_reports_for_date(
    target_date: str,
    download_pdf: bool = True,
    progress_callback=None,
) -> tuple[Path, int, int]:
    db_path = init_nfr_db(get_nfr_db_path(target_date))
    reports = collect_industry_report_links(target_date)
    total_count = len(reports)
    success_count = 0
    failed_count = 0

    for current_count, report in enumerate(reports, start=1):
        if progress_callback:
            progress_callback(current_count - 1, total_count, report, success_count, failed_count)

        try:
            report_data = extract_industry_report_detail(
                report=report,
                target_date=target_date,
                download_pdf=download_pdf,
            )
            save_industry_report_to_sqlite(report_data, db_path)
            success_count += 1
        except Exception:
            failed_count += 1

        if progress_callback:
            progress_callback(current_count, total_count, report, success_count, failed_count)

        time.sleep(0.1)

    return db_path, success_count, failed_count


st.title("네이버금융 리서치")

st.markdown(
    """
    이 페이지는 네이버금융 리서치 리포트를 날짜 기준으로 대량 수집합니다.

    1. 선택한 작성일의 종목분석 또는 산업분석 리포트 목록 페이지를 조회합니다.
    2. 마지막 페이지까지 순회하면서 모든 상세 리포트 URL을 수집합니다.
    3. 종목분석은 종목명, 리포트 제목, 증권사, 작성일, 조회수, 목표가, 투자의견, 본문 텍스트, PDF URL을 추출합니다.
    4. 산업분석은 업종명, 리포트 제목, 증권사, 작성일, 조회수, 본문 텍스트, PDF URL을 추출합니다.
    5. PDF 다운로드를 켜면 `data/NFR/company_reports/YYYY-MM-DD` 또는 `data/NFR/industry_reports/YYYY-MM-DD` 아래에 PDF 파일도 저장합니다.
    6. 추출 정보는 `data/NFR/nfr_YYYYMMDD.sqlite`의 `nfr_company_reports`, `nfr_industry_reports` 테이블에 저장합니다.
    """
)

selected_date = st.date_input(
    "리포트 작성일",
    value=date.today() - timedelta(days=1),
    max_value=date.today(),
)

download_pdf = st.checkbox("PDF 파일도 저장", value=True)

if st.button("종목분석 리포트 수집"):
    target_date = format_naver_date(selected_date)
    progress = st.progress(0)
    status = st.empty()

    def update_progress(
        current_count: int,
        total_count: int,
        report: dict[str, str],
        success_count: int,
        failed_count: int,
    ) -> None:
        progress.progress(current_count / total_count if total_count else 1.0)
        status.info(
            f"{current_count}/{total_count} 처리 중 - {report.get('list_title', '')} "
            f"(성공 {success_count}, 실패 {failed_count})"
        )

    try:
        with st.spinner("네이버금융 종목분석 리포트를 수집하는 중입니다."):
            db_path, success_count, failed_count = collect_reports_for_date(
                target_date=target_date,
                download_pdf=download_pdf,
                progress_callback=update_progress,
            )

        progress.progress(1.0)
        st.success(f"저장 완료: {db_path} / 성공 {success_count}건, 실패 {failed_count}건")

    except Exception as exc:
        progress.empty()
        status.empty()
        st.error(str(exc))

if st.button("산업분석 리포트 수집"):
    target_date = format_naver_date(selected_date)
    progress = st.progress(0)
    status = st.empty()

    def update_industry_progress(
        current_count: int,
        total_count: int,
        report: dict[str, str],
        success_count: int,
        failed_count: int,
    ) -> None:
        progress.progress(current_count / total_count if total_count else 1.0)
        status.info(
            f"{current_count}/{total_count} 처리 중 - {report.get('list_title', '')} "
            f"(성공 {success_count}, 실패 {failed_count})"
        )

    try:
        with st.spinner("네이버금융 산업분석 리포트를 수집하는 중입니다."):
            db_path, success_count, failed_count = collect_industry_reports_for_date(
                target_date=target_date,
                download_pdf=download_pdf,
                progress_callback=update_industry_progress,
            )

        progress.progress(1.0)
        st.success(f"저장 완료: {db_path} / 성공 {success_count}건, 실패 {failed_count}건")

    except Exception as exc:
        progress.empty()
        status.empty()
        st.error(str(exc))
