# =============================================================================
# config.py  —  Cấu hình kết nối DB và TMDb API
# =============================================================================
# Chỉnh sửa các giá trị dưới đây trước khi chạy ETL.
# =============================================================================

# ── PostgreSQL ────────────────────────────────────────────────────────────────
DB_CONFIG = {
    "dbname":   "PenguinDB",
    "user":     "postgres",
    "password": "1060812593",
    "host":     "localhost",
    "port":     "5432",
}

# ── TMDb API ──────────────────────────────────────────────────────────────────
TMDB_API_TOKEN = "eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiI4ZmUwZGEyM2U3MzQzNzExM2Y3MWNhY2RiNzJjOTIwZCIsIm5iZiI6MTc3Nzk3MzA4NS42OCwic3ViIjoiNjlmOWI3NWQwNjZlZTgyOTcxNjQ2N2EwIiwic2NvcGVzIjpbImFwaV9yZWFkIl0sInZlcnNpb24iOjF9.h8Ys_D7zMGqCs0eP0JgpBQMhhuEePCj4vkCaTmx31uI"

TMDB_BASE_URL  = "https://api.themoviedb.org/3"

# ── ETL Behaviour ─────────────────────────────────────────────────────────────
# True  → gọi API lấy chi tiết đầy đủ từng Person (thêm AKA, biography…)
#         Chú ý: mỗi movie_id có thể có 50–100 người → tốn nhiều request
# False → chỉ upsert thông tin tối thiểu có sẵn trong credits endpoint
FETCH_FULL_PERSON_DETAIL = False

# True  → import TMDb reviews vào User_Review (cần TMDB_SYSTEM_USER_ID tồn tại)
IMPORT_REVIEWS = False
# user_id của "hệ thống" đại diện cho TMDb reviews (phải tồn tại trong bảng "User")
TMDB_SYSTEM_USER_ID = 1

# Số giây chờ giữa các request API để tránh rate-limit (TMDb: 50 req/s)
API_DELAY_SECONDS = 0.05

# ── ETL Audit Log ─────────────────────────────────────────────────────────────
# True → ghi kết quả mỗi bước vào bảng ETL_Log
ENABLE_ETL_LOG = True
