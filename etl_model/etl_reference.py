# =============================================================================
# etl_reference.py  —  Load dữ liệu tham chiếu (static / lookup tables)
# =============================================================================
# Chạy MỘT LẦN (hoặc khi cần re-sync) trước khi ETL movie.
#
# Thứ tự bắt buộc:
#   1. Language
#   2. Country
#   3. Genre (movie)
#   4. Department  →  Job
#   5. Certification_Standard (movie)
#   6. Watch_Provider (movie)
# =============================================================================

import logging
from db_utils import tmdb_get, db_transaction, ETLLogger

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# 1. Language  ←  GET /configuration/languages
# ─────────────────────────────────────────────────────────────────────────────

def load_languages():
    """
    API trả về list[{iso_639_1, english_name, name}].
    Upsert vào bảng Language.
    """
    etl = ETLLogger("configuration/languages", media_type="reference")
    etl.start()

    data = tmdb_get("/configuration/languages")
    if not data:
        etl.finish("failed", error="API call returned None")
        return

    sql = """
        INSERT INTO Language (iso_639_1, english_name, native_name)
        VALUES (%s, %s, %s)
        ON CONFLICT (iso_639_1) DO UPDATE
            SET native_name = EXCLUDED.native_name
    """
    seen_codes = set()
    rows = []
    for item in data:
        code = item.get("iso_639_1")
        if not code or code in seen_codes:
            continue
        seen_codes.add(code)
        rows.append((
            code,
            item.get("english_name") or code,
            item.get("name") or ""
        ))

    try:
        with db_transaction() as (conn, cur):
            cur.executemany(sql, rows)
        logger.info("Language: upserted %d rows", len(rows))
        etl.finish("success", records=len(rows))
    except Exception as e:
        logger.error("Language load failed: %s", e)
        etl.finish("failed", error=str(e))


# ─────────────────────────────────────────────────────────────────────────────
# 2. Country  ←  GET /configuration/countries
# ─────────────────────────────────────────────────────────────────────────────

def load_countries():
    """
    API trả về list[{iso_3166_1, english_name, native_name}].
    """
    etl = ETLLogger("configuration/countries", media_type="reference")
    etl.start()

    data = tmdb_get("/configuration/countries", params={"language": "en-US"})
    if not data:
        etl.finish("failed", error="API call returned None")
        return

    sql = """
        INSERT INTO Country (iso_3166_1, english_name, native_name)
        VALUES (%s, %s, %s)
        ON CONFLICT (iso_3166_1) DO UPDATE
            SET native_name = EXCLUDED.native_name
    """
    seen_codes = set()
    rows = []
    for item in data:
        code = item.get("iso_3166_1")
        if not code or code in seen_codes:
            continue
        seen_codes.add(code)
        rows.append((
            code,
            item.get("english_name") or code,
            item.get("native_name") or ""
        ))

    try:
        with db_transaction() as (conn, cur):
            cur.executemany(sql, rows)
        logger.info("Country: upserted %d rows", len(rows))
        etl.finish("success", records=len(rows))
    except Exception as e:
        logger.error("Country load failed: %s", e)
        etl.finish("failed", error=str(e))


# ─────────────────────────────────────────────────────────────────────────────
# 3. Genre (movie)  ←  GET /genre/movie/list
# ─────────────────────────────────────────────────────────────────────────────

def load_genres_movie():
    """
    API trả về {genres: [{id, name}]}.
    Upsert vào Genre với media_type='movie'.
    """
    etl = ETLLogger("genre/movie/list", media_type="reference")
    etl.start()

    data = tmdb_get("/genre/movie/list", params={"language": "en"})
    if not data or "genres" not in data:
        etl.finish("failed", error="API call returned None or bad format")
        return

    sql = """
        INSERT INTO Genre (genre_id, name, media_type)
        VALUES (%s, %s, 'movie')
        ON CONFLICT (genre_id, media_type) DO UPDATE
            SET name = EXCLUDED.name
    """
    rows = [(g["id"], g["name"]) for g in data["genres"]]

    try:
        with db_transaction() as (conn, cur):
            cur.executemany(sql, rows)
        logger.info("Genre(movie): upserted %d rows", len(rows))
        etl.finish("success", records=len(rows))
    except Exception as e:
        logger.error("Genre(movie) load failed: %s", e)
        etl.finish("failed", error=str(e))


# ─────────────────────────────────────────────────────────────────────────────
# 4. Department + Job  ←  GET /configuration/jobs
# ─────────────────────────────────────────────────────────────────────────────
# API trả về list[{department: str, jobs: [str]}]
# → Department dùng SMALLSERIAL (auto-increment), không có tmdb_id.
#   Upsert theo department_name UNIQUE.
# → Job tương tự — upsert theo (department_id, job_name) UNIQUE.
# ─────────────────────────────────────────────────────────────────────────────

def load_departments_and_jobs():
    """
    Bước 1: upsert Department, lấy id.
    Bước 2: upsert Job với department_id tương ứng.
    """
    etl = ETLLogger("configuration/jobs", media_type="reference")
    etl.start()

    data = tmdb_get("/configuration/jobs")
    if not data:
        etl.finish("failed", error="API call returned None")
        return

    dept_sql = """
        INSERT INTO Department (department_name)
        VALUES (%s)
        ON CONFLICT (department_name) DO UPDATE
            SET department_name = EXCLUDED.department_name
        RETURNING department_id, department_name
    """
    job_sql = """
        INSERT INTO Job (department_id, job_name)
        VALUES (%s, %s)
        ON CONFLICT (department_id, job_name) DO NOTHING
    """

    total_depts = 0
    total_jobs  = 0

    try:
        with db_transaction() as (conn, cur):
            for entry in data:
                dept_name = entry.get("department", "").strip()
                jobs      = entry.get("jobs", [])
                if not dept_name:
                    continue

                # Upsert department, lấy về department_id
                cur.execute(dept_sql, (dept_name,))
                row = cur.fetchone()
                if row is None:
                    # ON CONFLICT DO UPDATE với RETURNING vẫn cần SELECT
                    cur.execute(
                        "SELECT department_id FROM Department WHERE department_name = %s",
                        (dept_name,)
                    )
                    row = cur.fetchone()
                dept_id = row[0]
                total_depts += 1

                # Upsert jobs của department đó
                job_rows = [(dept_id, j.strip()) for j in jobs if j.strip()]
                cur.executemany(job_sql, job_rows)
                total_jobs += len(job_rows)

        logger.info("Department: %d | Job: %d rows processed", total_depts, total_jobs)
        etl.finish("success", records=total_depts + total_jobs)
    except Exception as e:
        logger.error("Department/Job load failed: %s", e)
        etl.finish("failed", error=str(e))


# ─────────────────────────────────────────────────────────────────────────────
# 5. Certification_Standard (movie)  ←  GET /certification/movie/list
# ─────────────────────────────────────────────────────────────────────────────
# API trả về {certifications: {country_code: [{certification, meaning, order}]}}
# cert_order phải > 0 (CHECK constraint).  TMDb dùng 0-based → +1.
# ─────────────────────────────────────────────────────────────────────────────

def load_certifications_movie():
    etl = ETLLogger("certification/movie/list", media_type="reference")
    etl.start()

    data = tmdb_get("/certification/movie/list")
    if not data or "certifications" not in data:
        etl.finish("failed", error="API call returned None or bad format")
        return

    sql = """
        INSERT INTO Certification_Standard
            (iso_3166_1, certification, meaning, cert_order, media_type)
        VALUES (%s, %s, %s, %s, 'movie')
        ON CONFLICT (iso_3166_1, certification, media_type) DO UPDATE
            SET meaning     = EXCLUDED.meaning,
                cert_order  = EXCLUDED.cert_order
    """
    rows = []
    for country_code, certs in data["certifications"].items():
        if len(country_code) != 2:    
            continue                      
        for c in certs:
            raw_order = c.get("order", 0)
            cert_order = max(1, int(raw_order) + 1) if raw_order == 0 else max(1, int(raw_order))
            
            certification = (c.get("certification") or "")[:20]   # ← truncate 20 ký tự

            rows.append((
                country_code,
                certification, 
                c.get("meaning") or "",
                cert_order,
            ))

    try:
        with db_transaction() as (conn, cur):
            cur.executemany(sql, rows)
        logger.info("Certification_Standard(movie): upserted %d rows", len(rows))
        etl.finish("success", records=len(rows))
    except Exception as e:
        logger.error("Certification_Standard load failed: %s", e)
        etl.finish("failed", error=str(e))


# ─────────────────────────────────────────────────────────────────────────────
# 6. Watch_Provider (movie)  ←  GET /watch/providers/movie
# ─────────────────────────────────────────────────────────────────────────────
# API trả về {results: [{provider_id, provider_name, logo_path, ...}]}
# provider_id = tmdb_provider_id (dùng luôn làm PK)
# ─────────────────────────────────────────────────────────────────────────────

def load_watch_providers_movie():
    etl = ETLLogger("watch/providers/movie", media_type="reference")
    etl.start()

    data = tmdb_get("/watch/providers/movie", params={"language": "en-US"})
    if not data or "results" not in data:
        etl.finish("failed", error="API call returned None or bad format")
        return

    sql = """
        INSERT INTO Watch_Provider (provider_id, tmdb_provider_id, provider_name, logo_path)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (tmdb_provider_id) DO UPDATE
            SET provider_name = EXCLUDED.provider_name,
                logo_path     = EXCLUDED.logo_path
    """
    rows = [
        (
            p["provider_id"],          # provider_id = tmdb_provider_id (same)
            p["provider_id"],
            p.get("provider_name", ""),
            p.get("logo_path"),
        )
        for p in data["results"]
        if p.get("provider_id")
    ]

    try:
        with db_transaction() as (conn, cur):
            cur.executemany(sql, rows)
        logger.info("Watch_Provider(movie): upserted %d rows", len(rows))
        etl.finish("success", records=len(rows))
    except Exception as e:
        logger.error("Watch_Provider load failed: %s", e)
        etl.finish("failed", error=str(e))


# ─────────────────────────────────────────────────────────────────────────────
# Entry point: chạy tất cả reference loaders theo đúng thứ tự dependency
# ─────────────────────────────────────────────────────────────────────────────

def run_all_reference():
    """
    Chạy toàn bộ reference ETL theo đúng thứ tự FK dependency:
      Language → Country → Genre → Department → Job
                        → Certification_Standard → Watch_Provider
    """
    logger.info("=" * 60)
    logger.info("START: Reference Data ETL")
    logger.info("=" * 60)

    logger.info("[1/6] Loading Languages...")
    load_languages()

    logger.info("[2/6] Loading Countries...")
    load_countries()

    logger.info("[3/6] Loading Movie Genres...")
    load_genres_movie()

    logger.info("[4/6] Loading Departments & Jobs...")
    load_departments_and_jobs()

    logger.info("[5/6] Loading Movie Certifications...")
    load_certifications_movie()

    logger.info("[6/6] Loading Movie Watch Providers...")
    load_watch_providers_movie()

    logger.info("=" * 60)
    logger.info("DONE: Reference Data ETL")
    logger.info("=" * 60)
