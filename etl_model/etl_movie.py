# =============================================================================
# etl_movie.py  —  Full Movie ETL Pipeline (movie_id driven)
# =============================================================================
#
# Dataflow cho mỗi movie_id:
#   TMDb API  →  Collection (nếu có)
#             →  Movie  (bảng chính)
#             →  Movie_Genre, Movie_Country, Movie_Language, Movie_Company
#             →  Person (upsert)  →  Movie_Cast, Movie_Crew
#             →  Keyword (upsert) →  Movie_Keyword
#             →  Movie_Watch_Provider
#             →  (optional) User_Review
#
# Yêu cầu: Reference ETL đã chạy xong trước (Department, Job, Watch_Provider...)
# =============================================================================

import logging
from db_utils import (
    tmdb_get, db_transaction, ETLLogger,
    load_dept_job_maps, load_provider_map, load_cert_map
)
import config

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Helper: upsert một Person từ dict (dữ liệu tối giản từ credits)
# ─────────────────────────────────────────────────────────────────────────────

def _upsert_person_minimal(cur, p: dict):
    """
    Upsert Person với dữ liệu tối giản có trong credits.
    person_id = tmdb_person_id (thiết kế ERD: dùng luôn TMDb id làm PK nội bộ).
    """
    sql = """
        INSERT INTO Person (
            person_id, tmdb_person_id, name, original_name,
            gender, known_for_department, popularity,
            profile_path, adult
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (tmdb_person_id) DO UPDATE
            SET name                 = EXCLUDED.name,
                original_name        = EXCLUDED.original_name,
                gender               = EXCLUDED.gender,
                known_for_department = EXCLUDED.known_for_department,
                popularity           = EXCLUDED.popularity,
                profile_path         = EXCLUDED.profile_path,
                adult                = EXCLUDED.adult,
                updated_at           = NOW()
    """
    gender = p.get("gender")
    if gender not in (0, 1, 2, 3):
        gender = 0
    cur.execute(sql, (
        p["id"],                          # person_id
        p["id"],                          # tmdb_person_id
        p.get("name", ""),
        p.get("original_name"),
        gender,
        p.get("known_for_department"),
        p.get("popularity") or 0,
        p.get("profile_path"),
        bool(p.get("adult", False)),
    ))


def _upsert_person_full(cur, person_id: int):
    """
    Gọi API GET /person/{person_id} để lấy đầy đủ thông tin,
    rồi upsert Person + Person_AKA.
    Chỉ gọi khi FETCH_FULL_PERSON_DETAIL=True.
    """
    data = tmdb_get(f"/person/{person_id}", params={"language": "en-US"})
    if not data:
        return

    gender = data.get("gender")
    if gender not in (0, 1, 2, 3):
        gender = 0

    # ── Person ──────────────────────────────────────────────────────────────
    sql_person = """
        INSERT INTO Person (
            person_id, tmdb_person_id, name, original_name, biography,
            birthday, deathday, gender, known_for_department,
            place_of_birth, popularity, profile_path, homepage,
            imdb_id, adult, etl_synced_at
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
        ON CONFLICT (tmdb_person_id) DO UPDATE
            SET name                 = EXCLUDED.name,
                original_name        = EXCLUDED.original_name,
                biography            = EXCLUDED.biography,
                birthday             = EXCLUDED.birthday,
                deathday             = EXCLUDED.deathday,
                gender               = EXCLUDED.gender,
                known_for_department = EXCLUDED.known_for_department,
                place_of_birth       = EXCLUDED.place_of_birth,
                popularity           = EXCLUDED.popularity,
                profile_path         = EXCLUDED.profile_path,
                homepage             = EXCLUDED.homepage,
                imdb_id              = EXCLUDED.imdb_id,
                adult                = EXCLUDED.adult,
                etl_synced_at        = NOW(),
                updated_at           = NOW()
    """
    def _safe_date(s):
        return s if s else None

    cur.execute(sql_person, (
        data["id"], data["id"],
        data.get("name", ""),
        data.get("name"),                  # original_name = name nếu không có
        data.get("biography"),
        _safe_date(data.get("birthday")),
        _safe_date(data.get("deathday")),
        gender,
        data.get("known_for_department"),
        data.get("place_of_birth"),
        data.get("popularity") or 0,
        data.get("profile_path"),
        data.get("homepage"),
        data.get("imdb_id"),
        bool(data.get("adult", False)),
    ))

    # ── Person_AKA ───────────────────────────────────────────────────────────
    # Xóa AKA cũ, insert lại (danh sách có thể thay đổi)
    cur.execute("DELETE FROM Person_AKA WHERE person_id = %s", (data["id"],))
    akas = data.get("also_known_as") or []
    if akas:
        aka_rows = [(data["id"], alias) for alias in akas if alias]
        cur.executemany(
            "INSERT INTO Person_AKA (person_id, alias) VALUES (%s, %s)",
            aka_rows
        )


# ─────────────────────────────────────────────────────────────────────────────
# Helper: upsert Company
# ─────────────────────────────────────────────────────────────────────────────

def _upsert_company(cur, c: dict):
    """Upsert Company từ production_companies entry trong movie.json."""
    sql = """
        INSERT INTO Company (company_id, tmdb_company_id, name, logo_path, origin_country)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (tmdb_company_id) DO UPDATE
            SET name           = EXCLUDED.name,
                logo_path      = EXCLUDED.logo_path,
                origin_country = EXCLUDED.origin_country
    """
    origin = c.get("origin_country") or None
    if origin == "":
        origin = None
    cur.execute(sql, (
        c["id"], c["id"],
        c.get("name", ""),
        c.get("logo_path"),
        origin,
    ))


# ─────────────────────────────────────────────────────────────────────────────
# Bước 1: Collection  ←  belongs_to_collection trong movie.json
# ─────────────────────────────────────────────────────────────────────────────

def _upsert_collection(cur, col: dict):
    """col = data['belongs_to_collection'] (có thể None)."""
    if not col or not col.get("id"):
        return
    sql = """
        INSERT INTO Collection (
            collection_id, tmdb_collection_id, name,
            poster_path, backdrop_path
        )
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (tmdb_collection_id) DO UPDATE
            SET name          = EXCLUDED.name,
                poster_path   = EXCLUDED.poster_path,
                backdrop_path = EXCLUDED.backdrop_path
    """
    cur.execute(sql, (
        col["id"], col["id"],
        col.get("name", ""),
        col.get("poster_path"),
        col.get("backdrop_path"),
    ))


# ─────────────────────────────────────────────────────────────────────────────
# Bước 2: Movie + direct junctions (Genre, Country, Language, Company)
#          ←  GET /movie/{movie_id}
# ─────────────────────────────────────────────────────────────────────────────

def _load_movie_core(cur, movie_id: int):
    """
    Gọi API, upsert Collection + Movie + Movie_Genre + Movie_Country
    + Movie_Language + Movie_Company.
    Trả về (success: bool, movie_data: dict).
    """
    data = tmdb_get(f"/movie/{movie_id}", params={"language": "en-US"})
    if not data or "id" not in data:
        return False, None

    # ── Collection (nếu có) ──────────────────────────────────────────────────
    btc = data.get("belongs_to_collection")
    _upsert_collection(cur, btc)
    collection_db_id = btc["id"] if btc else None

    # ── Movie ────────────────────────────────────────────────────────────────
    def _safe(v, default=None):
        return v if v is not None else default

    # Validate status
    valid_statuses = {
        "Rumored", "Planned", "In Production",
        "Post Production", "Released", "Canceled"
    }
    status = data.get("status")
    if status not in valid_statuses:
        status = None

    movie_sql = """
        INSERT INTO Movie (
            movie_id, tmdb_movie_id, imdb_id, title, original_title,
            original_language, overview, tagline, release_date, status,
            revenue, budget, runtime, popularity, vote_average, vote_count,
            poster_path, backdrop_path, homepage, adult,
            collection_id, etl_synced_at
        )
        VALUES (
            %s,%s,%s,%s,%s, %s,%s,%s,%s,%s,
            %s,%s,%s,%s,%s,%s,
            %s,%s,%s,%s, %s,NOW()
        )
        ON CONFLICT (tmdb_movie_id) DO UPDATE
            SET imdb_id           = EXCLUDED.imdb_id,
                title             = EXCLUDED.title,
                original_title    = EXCLUDED.original_title,
                original_language = EXCLUDED.original_language,
                overview          = EXCLUDED.overview,
                tagline           = EXCLUDED.tagline,
                release_date      = EXCLUDED.release_date,
                status            = EXCLUDED.status,
                revenue           = EXCLUDED.revenue,
                budget            = EXCLUDED.budget,
                runtime           = EXCLUDED.runtime,
                popularity        = EXCLUDED.popularity,
                vote_average      = EXCLUDED.vote_average,
                vote_count        = EXCLUDED.vote_count,
                poster_path       = EXCLUDED.poster_path,
                backdrop_path     = EXCLUDED.backdrop_path,
                homepage          = EXCLUDED.homepage,
                adult             = EXCLUDED.adult,
                collection_id     = EXCLUDED.collection_id,
                etl_synced_at     = NOW(),
                updated_at        = NOW()
    """
    release_date = data.get("release_date") or None
    if release_date == "":
        release_date = None

    cur.execute(movie_sql, (
        data["id"], data["id"],
        data.get("imdb_id"),
        data.get("title", ""),
        data.get("original_title", ""),
        data.get("original_language", "en"),
        data.get("overview"),
        data.get("tagline"),
        release_date,
        status,
        _safe(data.get("revenue"), 0),
        _safe(data.get("budget"), 0),
        data.get("runtime"),
        _safe(data.get("popularity"), 0),
        _safe(data.get("vote_average"), 0),
        _safe(data.get("vote_count"), 0),
        data.get("poster_path"),
        data.get("backdrop_path"),
        data.get("homepage"),
        bool(data.get("adult", False)),
        collection_db_id,
    ))

    mid = data["id"]

    # ── Movie_Genre ──────────────────────────────────────────────────────────
    # Xóa rồi insert lại (diff đơn giản nhất)
    cur.execute("DELETE FROM Movie_Genre WHERE movie_id = %s", (mid,))
    for g in (data.get("genres") or []):
        if g.get("id"):
            cur.execute(
                """INSERT INTO Movie_Genre (movie_id, genre_id)
                   VALUES (%s,%s) ON CONFLICT DO NOTHING""",
                (mid, g["id"])
            )

    # ── Movie_Country (production_countries) ────────────────────────────────
    cur.execute("DELETE FROM Movie_Country WHERE movie_id = %s", (mid,))
    for c in (data.get("production_countries") or []):
        if c.get("iso_3166_1"):
            cur.execute(
                """INSERT INTO Movie_Country (movie_id, iso_3166_1)
                   VALUES (%s,%s) ON CONFLICT DO NOTHING""",
                (mid, c["iso_3166_1"])
            )

    # ── Movie_Language (spoken_languages + original) ─────────────────────────
    cur.execute("DELETE FROM Movie_Language WHERE movie_id = %s", (mid,))
    orig_lang = data.get("original_language", "en")
    # Insert original language
    cur.execute(
        """INSERT INTO Movie_Language (movie_id, iso_639_1, language_type)
           VALUES (%s,%s,'original') ON CONFLICT DO NOTHING""",
        (mid, orig_lang)
    )
    # Insert spoken languages
    for lang in (data.get("spoken_languages") or []):
        code = lang.get("iso_639_1")
        if code and code != orig_lang:
            cur.execute(
                """INSERT INTO Movie_Language (movie_id, iso_639_1, language_type)
                   VALUES (%s,%s,'spoken') ON CONFLICT DO NOTHING""",
                (mid, code)
            )

    # ── Company + Movie_Company ──────────────────────────────────────────────
    cur.execute("DELETE FROM Movie_Company WHERE movie_id = %s", (mid,))
    for comp in (data.get("production_companies") or []):
        if comp.get("id"):
            _upsert_company(cur, comp)
            cur.execute(
                """INSERT INTO Movie_Company (movie_id, company_id)
                   VALUES (%s,%s) ON CONFLICT DO NOTHING""",
                (mid, comp["id"])
            )

    return True, data


# ─────────────────────────────────────────────────────────────────────────────
# Bước 3: Credits (Cast + Crew)  ←  GET /movie/{movie_id}/credits
# ─────────────────────────────────────────────────────────────────────────────

def _load_movie_credits(cur, movie_id: int, dept_map: dict, job_map: dict):
    """
    Upsert Person (minimal hoặc full).
    Xóa + insert lại Movie_Cast, Movie_Crew.
    dept_map: {dept_name -> dept_id}
    job_map:  {(dept_name, job_name) -> job_id}
    """
    data = tmdb_get(f"/movie/{movie_id}/credits", params={"language": "en-US"})
    if not data:
        return 0

    mid   = movie_id
    count = 0

    # ── CAST ─────────────────────────────────────────────────────────────────
    cur.execute("DELETE FROM Movie_Cast WHERE movie_id = %s", (mid,))

    for member in (data.get("cast") or []):
        pid = member.get("id")
        if not pid:
            continue

        # Upsert person
        if config.FETCH_FULL_PERSON_DETAIL:
            _upsert_person_full(cur, pid)
        else:
            _upsert_person_minimal(cur, member)

        # TMDb order bắt đầu từ 0; DB constraint cast_order > 0 → +1
        cast_order = int(member.get("order", 0)) + 1

        cur.execute(
            """
            INSERT INTO Movie_Cast (movie_id, person_id, cast_order, character_name, credit_id)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (movie_id, person_id, cast_order) DO UPDATE
                SET character_name = EXCLUDED.character_name,
                    credit_id      = EXCLUDED.credit_id
            """,
            (
                mid, pid, cast_order,
                member.get("character") or "",
                member.get("credit_id"),
            )
        )
        count += 1

    # ── CREW ─────────────────────────────────────────────────────────────────
    cur.execute("DELETE FROM Movie_Crew WHERE movie_id = %s", (mid,))

    for member in (data.get("crew") or []):
        pid       = member.get("id")
        dept_name = (member.get("department") or "").strip()
        job_name  = (member.get("job") or "").strip()
        if not pid or not dept_name or not job_name:
            continue

        # Lookup department_id và job_id từ map đã build
        dept_id = dept_map.get(dept_name)
        job_id  = job_map.get((dept_name, job_name))

        if dept_id is None or job_id is None:
            # Dept/Job chưa tồn tại trong DB → insert động
            if dept_id is None:
                cur.execute(
                    """INSERT INTO Department (department_name)
                       VALUES (%s) ON CONFLICT (department_name) DO UPDATE
                       SET department_name=EXCLUDED.department_name
                       RETURNING department_id""",
                    (dept_name,)
                )
                row = cur.fetchone()
                if row is None:
                    cur.execute(
                        "SELECT department_id FROM Department WHERE department_name=%s",
                        (dept_name,)
                    )
                    row = cur.fetchone()
                dept_id = row[0]
                dept_map[dept_name] = dept_id        # cập nhật cache

            if job_id is None:
                cur.execute(
                    """INSERT INTO Job (department_id, job_name)
                       VALUES (%s,%s) ON CONFLICT (department_id, job_name) DO NOTHING
                       RETURNING job_id""",
                    (dept_id, job_name)
                )
                row = cur.fetchone()
                if row is None:
                    cur.execute(
                        "SELECT job_id FROM Job WHERE department_id=%s AND job_name=%s",
                        (dept_id, job_name)
                    )
                    row = cur.fetchone()
                if row:
                    job_id = row[0]
                    job_map[(dept_name, job_name)] = job_id
                else:
                    continue   # Không thể insert job → skip

        # Upsert person
        if config.FETCH_FULL_PERSON_DETAIL:
            _upsert_person_full(cur, pid)
        else:
            _upsert_person_minimal(cur, member)

        cur.execute(
            """
            INSERT INTO Movie_Crew (movie_id, person_id, department_id, job_id, credit_id)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (movie_id, person_id, department_id, job_id) DO UPDATE
                SET credit_id = EXCLUDED.credit_id
            """,
            (mid, pid, dept_id, job_id, member.get("credit_id"))
        )
        count += 1

    return count


# ─────────────────────────────────────────────────────────────────────────────
# Bước 4: Keywords  ←  GET /movie/{movie_id}/keywords
# ─────────────────────────────────────────────────────────────────────────────

def _load_movie_keywords(cur, movie_id: int):
    data = tmdb_get(f"/movie/{movie_id}/keywords")
    if not data:
        return 0

    mid   = movie_id
    count = 0

    cur.execute("DELETE FROM Movie_Keyword WHERE movie_id = %s", (mid,))

    for kw in (data.get("keywords") or []):
        kid = kw.get("id")
        if not kid:
            continue
        # Upsert keyword
        cur.execute(
            """INSERT INTO Keyword (keyword_id, name)
               VALUES (%s,%s) ON CONFLICT (keyword_id) DO UPDATE SET name=EXCLUDED.name""",
            (kid, kw.get("name", ""))
        )
        # Link to movie
        cur.execute(
            """INSERT INTO Movie_Keyword (movie_id, keyword_id)
               VALUES (%s,%s) ON CONFLICT DO NOTHING""",
            (mid, kid)
        )
        count += 1

    return count


# ─────────────────────────────────────────────────────────────────────────────
# Bước 5: Watch Providers  ←  GET /movie/{movie_id}/watch/providers
# ─────────────────────────────────────────────────────────────────────────────

def _load_movie_watch_providers(cur, movie_id: int, provider_map: dict):
    """
    results: { country_code: { flatrate/rent/buy: [{provider_id, display_priority}] } }
    availability_type IN ('flatrate','rent','buy','free','ads') (CHECK constraint)
    """
    data = tmdb_get(f"/movie/{movie_id}/watch/providers")
    if not data or "results" not in data:
        return 0

    mid   = movie_id
    count = 0
    valid_types = {"flatrate", "rent", "buy", "free", "ads"}

    cur.execute("DELETE FROM Movie_Watch_Provider WHERE movie_id = %s", (mid,))

    for country_code, avail in data["results"].items():
        if len(country_code) != 2:
            continue
    
    # ← thêm đoạn này: đảm bảo country tồn tại trước khi insert FK
        cur.execute(
            """INSERT INTO Country (iso_3166_1, english_name, native_name)
                VALUES (%s, %s, '')
                ON CONFLICT (iso_3166_1) DO NOTHING""",
            (country_code, country_code)
        )
        for avail_type, providers in avail.items():
            if avail_type not in valid_types or not isinstance(providers, list):
                continue
            for p in providers:
                tid = p.get("provider_id")
                if not tid:
                    continue

                # Upsert provider nếu chưa có trong bảng (trường hợp provider_map cũ)
                if tid not in provider_map:
                    cur.execute(
                        """INSERT INTO Watch_Provider (provider_id, tmdb_provider_id,
                               provider_name, logo_path)
                           VALUES (%s,%s,%s,%s)
                           ON CONFLICT (tmdb_provider_id) DO NOTHING""",
                        (tid, tid, p.get("provider_name", ""), p.get("logo_path"))
                    )
                    provider_map[tid] = tid

                cur.execute(
                    """
                    INSERT INTO Movie_Watch_Provider
                        (movie_id, provider_id, iso_3166_1, availability_type, display_priority)
                    VALUES (%s,%s,%s,%s,%s)
                    ON CONFLICT (movie_id, provider_id, iso_3166_1, availability_type)
                    DO UPDATE SET display_priority = EXCLUDED.display_priority
                    """,
                    (mid, tid, country_code, avail_type,
                     p.get("display_priority", 0))
                )
                count += 1

    return count


# ─────────────────────────────────────────────────────────────────────────────
# Bước 6 (optional): Reviews  ←  GET /movie/{movie_id}/reviews
# ─────────────────────────────────────────────────────────────────────────────

def _load_movie_reviews(cur, movie_id: int):
    """
    Import TMDb reviews dưới dạng User_Review với user_id = TMDB_SYSTEM_USER_ID.
    Chỉ chạy khi config.IMPORT_REVIEWS=True.
    """
    if not config.IMPORT_REVIEWS:
        return 0

    data = tmdb_get(
        f"/movie/{movie_id}/reviews",
        params={"language": "en-US", "page": 1}
    )
    if not data or "results" not in data:
        return 0

    mid   = movie_id
    count = 0

    for rev in (data.get("results") or []):
        rid = rev.get("id")
        if not rid:
            continue

        rating_raw = (rev.get("author_details") or {}).get("rating")
        if rating_raw is not None:
            # Làm tròn về bước 0.5 trong range [0.5, 10.0]
            rating = max(0.5, min(10.0, round(float(rating_raw) * 2) / 2))
        else:
            rating = None

        created = rev.get("created_at")
        if created:
            created = created[:26]   # cắt về microsecond nếu cần

        cur.execute(
            """
            INSERT INTO User_Review
                (tmdb_review_id, user_id, media_type, movie_id,
                 content, rating, tmdb_url, created_at)
            VALUES (%s,%s,'movie',%s,%s,%s,%s,
                    COALESCE(%s::TIMESTAMPTZ, NOW()))
            ON CONFLICT (tmdb_review_id) DO UPDATE
                SET content    = EXCLUDED.content,
                    rating     = EXCLUDED.rating,
                    updated_at = NOW()
            """,
            (
                rid,
                config.TMDB_SYSTEM_USER_ID,
                mid,
                rev.get("content", ""),
                rating,
                rev.get("url"),
                created,
            )
        )
        count += 1

    return count


# ─────────────────────────────────────────────────────────────────────────────
# Main: ETL một movie_id đầy đủ
# ─────────────────────────────────────────────────────────────────────────────

def run_movie_etl(movie_id: int):
    """
    ETL đầy đủ cho một movie_id.
    Toàn bộ chạy trong một transaction — nếu bất kỳ bước nào lỗi,
    transaction rollback, không để dữ liệu nửa vời.

    Returns: True nếu thành công, False nếu thất bại.
    """
    etl = ETLLogger(f"movie/{movie_id}", tmdb_id=movie_id, media_type="movie")
    etl.start()

    logger.info("  → [movie] id=%d: bắt đầu ETL...", movie_id)

    try:
        conn = __import__("db_utils").get_connection()
        try:
            with conn:
                with conn.cursor() as cur:

                    # ── Load lookup maps từ DB ────────────────────────────────
                    dept_map, job_map = load_dept_job_maps(conn)
                    provider_map      = load_provider_map(conn)

                    # ── Bước 1+2: Collection + Movie + junctions ──────────────
                    ok, movie_data = _load_movie_core(cur, movie_id)
                    if not ok:
                        raise ValueError(f"movie/{movie_id}: API trả về None hoặc lỗi")

                    records = 1   # 1 movie

                    # ── Bước 3: Credits ───────────────────────────────────────
                    n = _load_movie_credits(cur, movie_id, dept_map, job_map)
                    logger.info("    credits: %d người", n)
                    records += n

                    # ── Bước 4: Keywords ──────────────────────────────────────
                    n = _load_movie_keywords(cur, movie_id)
                    logger.info("    keywords: %d", n)
                    records += n

                    # ── Bước 5: Watch Providers ───────────────────────────────
                    n = _load_movie_watch_providers(cur, movie_id, provider_map)
                    logger.info("    watch_providers: %d links", n)
                    records += n

                    # ── Bước 6: Reviews (optional) ────────────────────────────
                    n = _load_movie_reviews(cur, movie_id)
                    if n:
                        logger.info("    reviews: %d", n)
                    records += n

            logger.info("  ✓ [movie] id=%d: DONE (%d records)", movie_id, records)
            etl.finish("success", records=records)
            return True

        except Exception as e:
            conn.rollback()
            logger.error("  ✗ [movie] id=%d: FAILED — %s", movie_id, e)
            etl.finish("failed", error=str(e))
            return False
        finally:
            conn.close()

    except Exception as e:
        logger.error("  ✗ [movie] id=%d: DB connection error — %s", movie_id, e)
        etl.finish("failed", error=str(e))
        return False


# ─────────────────────────────────────────────────────────────────────────────
# Batch: chạy danh sách movie_id
# ─────────────────────────────────────────────────────────────────────────────

def run_movies_etl(movie_ids: list[int], stop_on_error: bool = False):
    """
    Chạy ETL cho nhiều movie_id.
    stop_on_error=True → dừng ngay khi gặp lỗi đầu tiên.
    """
    success = 0
    failed  = 0

    logger.info("=" * 60)
    logger.info("START: Movie ETL — %d movie(s)", len(movie_ids))
    logger.info("=" * 60)

    for mid in movie_ids:
        ok = run_movie_etl(mid)
        if ok:
            success += 1
        else:
            failed += 1
            if stop_on_error:
                logger.error("stop_on_error=True: dừng tại movie_id=%d", mid)
                break

    logger.info("=" * 60)
    logger.info("DONE: success=%d / failed=%d / total=%d",
                success, failed, success + failed)
    logger.info("=" * 60)
    return success, failed
