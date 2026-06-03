# TMDb → PostgreSQL ETL Pipeline

## Cấu trúc file

```
etl_model/
├── config.py          ← Cấu hình (sửa trước khi chạy)
├── db_utils.py        ← DB connection, API client, ETL_Log
├── etl_reference.py   ← Nạp dữ liệu tĩnh (lookup tables)
├── etl_movie.py       ← ETL đầy đủ cho Movie
└── run_etl.py         ← CLI entry point (chạy từ đây)
```

---

## Bước 0 — Cài đặt

```bash
pip install psycopg2-binary requests
```

---

## Bước 1 — Cấu hình (`config.py`)

```python
DB_CONFIG = {
    "dbname":   "penguinDB",
    "user":     "postgres",
    "password": "YOUR_PASSWORD",
    "host":     "localhost",
    "port":     "5432",
}
TMDB_API_TOKEN = "eyJhbGc..."   # Bearer token từ TMDb Settings → API
```

---

## Bước 2 — Chạy Reference ETL (1 lần)

```bash
python run_etl.py --mode reference
```

Nạp theo thứ tự:

| # | Bảng | Endpoint TMDb |
|---|------|---------------|
| 1 | Language | `/configuration/languages` |
| 2 | Country | `/configuration/countries` |
| 3 | Genre (movie) | `/genre/movie/list` |
| 4 | Department + Job | `/configuration/jobs` |
| 5 | Certification_Standard | `/certification/movie/list` |
| 6 | Watch_Provider | `/watch/providers/movie` |

---

## Bước 3 — Chạy Movie ETL

```bash
# Một phim (movie_id = TMDb id trên URL phim)
python run_etl.py --mode movie --id 550

# Nhiều phim
python run_etl.py --mode movie --id 550 278 238 680 13 424

# Cả hai (reference + movies) trong một lệnh
python run_etl.py --mode all --id 550 278
```

---

## Dataflow chi tiết (mỗi `movie_id`)

```
GET /movie/{id}
  → upsert Collection (nếu có belongs_to_collection)
  → upsert Movie
  → delete+insert Movie_Genre
  → delete+insert Movie_Country
  → delete+insert Movie_Language  (original + spoken)
  → upsert Company → delete+insert Movie_Company

GET /movie/{id}/credits
  → upsert Person (minimal hoặc full — xem config)
  → delete+insert Movie_Cast  (cast_order = TMDb order + 1)
  → delete+insert Movie_Crew  (lookup dept_id + job_id)

GET /movie/{id}/keywords
  → upsert Keyword
  → delete+insert Movie_Keyword

GET /movie/{id}/watch/providers
  → delete+insert Movie_Watch_Provider (per country, per type)

GET /movie/{id}/reviews  [nếu IMPORT_REVIEWS=True]
  → upsert User_Review (user_id = TMDB_SYSTEM_USER_ID)
```

---

## Flags

| Flag | Mặc định | Mô tả |
|------|----------|-------|
| `--stop-on-error` | False | Dừng batch khi gặp lỗi đầu tiên |
| `--no-log` | False | Không ghi ETL_Log vào DB |
| `--debug` | False | Bật DEBUG logging |

---

## Import từ Python code

```python
from etl_reference import run_all_reference
from etl_movie import run_movie_etl, run_movies_etl

# Nạp lookup tables
run_all_reference()

# ETL một phim
run_movie_etl(550)

# ETL nhiều phim
success, failed = run_movies_etl([550, 278, 238], stop_on_error=False)
print(f"success={success}, failed={failed}")
```

---

## Config nâng cao

```python
# config.py

# True = gọi thêm GET /person/{id} cho mỗi người → biography, AKA, v.v.
# False = chỉ dùng dữ liệu trong /credits (nhanh hơn nhiều)
FETCH_FULL_PERSON_DETAIL = False

# True = import TMDb reviews vào User_Review
IMPORT_REVIEWS = False
TMDB_SYSTEM_USER_ID = 1   # user_id đại diện cho TMDb reviews

# Ghi ETL_Log sau mỗi bước
ENABLE_ETL_LOG = True
```

---

## Lưu ý

- `movie_id` = `tmdb_movie_id`: theo thiết kế ERD, dùng luôn TMDb ID làm PK nội bộ.
- `cast_order`: TMDb dùng 0-based → ETL tự cộng +1 để thỏa `CHECK (cast_order > 0)`.
- Toàn bộ một movie_id chạy trong **một transaction** — lỗi ở bất kỳ bước nào sẽ rollback toàn bộ.
- `Movie_Certification`: chưa implement vì TMDb lưu cert theo `release_dates` endpoint (không có trong API links hiện tại).
- TV Series: chưa implement, thiết kế tương tự Movie — xem `etl_movie.py` để mở rộng.
