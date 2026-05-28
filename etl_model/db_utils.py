# =============================================================================
# db_utils.py  —  Kết nối DB, TMDb API client, ETL_Log helper
# =============================================================================

import time
import logging
from contextlib import contextmanager
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras
import requests
    
import config

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# 1. DB Connection
# ─────────────────────────────────────────────────────────────────────────────

def get_connection():
    """Trả về psycopg2 connection mới.  Caller chịu trách nhiệm đóng."""
    return psycopg2.connect(**config.DB_CONFIG)


@contextmanager
def db_transaction():
    """Context manager: mở conn + cursor, tự commit/rollback, đóng conn."""
    conn = get_connection()
    try:
        with conn:                          # auto-commit khi thoát, rollback khi exception
            with conn.cursor() as cur:
                yield conn, cur
    finally:
        conn.close()


# ─────────────────────────────────────────────────────────────────────────────
# 2. TMDb API Client
# ─────────────────────────────────────────────────────────────────────────────

_SESSION = None


def _get_session() -> requests.Session:
    global _SESSION
    if _SESSION is None:
        _SESSION = requests.Session()
        _SESSION.headers.update({
            "Authorization": f"Bearer {config.TMDB_API_TOKEN}",
            "accept": "application/json",
        })
    return _SESSION


def tmdb_get(path: str, params: dict = None) -> dict | list | None:
    """
    Gọi TMDb API GET.  Trả về parsed JSON hoặc None nếu lỗi.
    path: phần sau base URL, VD '/movie/550'
    """
    url = config.TMDB_BASE_URL + path
    try:
        resp = _get_session().get(url, params=params, timeout=15)
        resp.raise_for_status()
        time.sleep(config.API_DELAY_SECONDS)
        return resp.json()
    except requests.HTTPError as e:
        logger.error("TMDb HTTP error %s for %s: %s", resp.status_code, url, e)
        return None
    except Exception as e:
        logger.error("TMDb request failed for %s: %s", url, e)
        return None


# ─────────────────────────────────────────────────────────────────────────────
# 3. ETL_Log helper
# ─────────────────────────────────────────────────────────────────────────────

class ETLLogger:
    """Ghi kết quả ETL vào bảng ETL_Log (nếu ENABLE_ETL_LOG=True)."""

    def __init__(self, endpoint: str, tmdb_id: int = None, media_type: str = None):
        self.endpoint    = endpoint
        self.tmdb_id     = tmdb_id
        self.media_type  = media_type
        self.started_at  = datetime.now(timezone.utc)
        self.log_id      = None

    # -- ghi bản ghi started --------------------------------------------------
    def start(self):
        if not config.ENABLE_ETL_LOG:
            return
        sql = """
            INSERT INTO ETL_Log (endpoint, tmdb_id, media_type, status,
                                 records_processed, started_at)
            VALUES (%s, %s, %s, 'partial', 0, %s)
            RETURNING log_id
        """
        try:
            with db_transaction() as (conn, cur):
                cur.execute(sql, (self.endpoint, self.tmdb_id,
                                  self.media_type, self.started_at))
                self.log_id = cur.fetchone()[0]
        except Exception as e:
            logger.warning("ETLLogger.start failed: %s", e)

    # -- cập nhật kết quả khi xong --------------------------------------------
    def finish(self, status: str, records: int = 0, error: str = None):
        if not config.ENABLE_ETL_LOG or self.log_id is None:
            return
        sql = """
            UPDATE ETL_Log
            SET status = %s, records_processed = %s,
                error_message = %s, finished_at = %s
            WHERE log_id = %s
        """
        try:
            with db_transaction() as (conn, cur):
                cur.execute(sql, (status, records, error,
                                  datetime.now(timezone.utc), self.log_id))
        except Exception as e:
            logger.warning("ETLLogger.finish failed: %s", e)


# ─────────────────────────────────────────────────────────────────────────────
# 4. Lookup maps (built once, reused by movie ETL)
# ─────────────────────────────────────────────────────────────────────────────

def load_dept_job_maps(conn) -> tuple[dict, dict]:
    """
    Trả về:
      dept_map  : { department_name  -> department_id  }
      job_map   : { (dept_name, job_name) -> job_id }
    Đọc trực tiếp từ DB (sau khi reference ETL đã chạy).
    """
    with conn.cursor() as cur:
        cur.execute("SELECT department_id, department_name FROM Department")
        dept_map = {row[1]: row[0] for row in cur.fetchall()}

        cur.execute("""
            SELECT j.job_id, d.department_name, j.job_name
            FROM   Job j
            JOIN   Department d ON d.department_id = j.department_id
        """)
        job_map = {(row[1], row[2]): row[0] for row in cur.fetchall()}

    return dept_map, job_map


def load_cert_map(conn) -> dict:
    """
    Trả về: { (iso_3166_1, certification, media_type) -> cert_std_id }
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT cert_std_id, iso_3166_1, certification, media_type
            FROM   Certification_Standard
        """)
        return {(r[1], r[2], r[3]): r[0] for r in cur.fetchall()}


def load_provider_map(conn) -> dict:
    """
    Trả về: { tmdb_provider_id -> provider_id (DB) }
    (ở đây provider_id = tmdb_provider_id)
    """
    with conn.cursor() as cur:
        cur.execute("SELECT provider_id, tmdb_provider_id FROM Watch_Provider")
        return {r[1]: r[0] for r in cur.fetchall()}
