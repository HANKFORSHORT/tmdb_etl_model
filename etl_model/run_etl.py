#!/usr/bin/env python3
# =============================================================================
# run_etl.py  —  CLI Entry Point cho TMDb → PostgreSQL ETL
# =============================================================================
#
# HƯỚNG DẪN SỬ DỤNG
# ──────────────────
#
# 1) Cài dependencies (một lần):
#       pip install psycopg2-binary requests
#
# 2) Chỉnh sửa config.py:
#       - DB_CONFIG     : host, dbname, user, password, port
#       - TMDB_API_TOKEN: Bearer token từ https://www.themoviedb.org/settings/api
#
# 3) Chạy Reference ETL TRƯỚC (chỉ cần chạy 1 lần, hoặc khi cần re-sync):
#       python run_etl.py --mode reference
#
# 4) Chạy Movie ETL cho một movie:
#       python run_etl.py --mode movie --id 550
#       (550 = Fight Club — lấy tmdb_movie_id từ URL phim trên TMDb)
#
# 5) Chạy Movie ETL cho nhiều movie:
#       python run_etl.py --mode movie --id 550 278 238 680 13
#
# 6) Chạy cả hai (reference + movies) trong một lệnh:
#       python run_etl.py --mode all --id 550 278 238
#
# 7) Chạy từ Python code (import trực tiếp):
#       from etl_reference import run_all_reference
#       from etl_movie import run_movie_etl, run_movies_etl
#
#       run_all_reference()               # nạp lookup tables
#       run_movie_etl(550)                # một phim
#       run_movies_etl([550, 278, 238])   # nhiều phim
#
# FLAGS TÙY CHỌN
# ──────────────
#   --stop-on-error   Dừng ngay khi một movie ETL thất bại (mặc định: tiếp tục)
#   --no-log          Không ghi ETL_Log vào DB
#   --debug           Hiển thị DEBUG logs
#
# VÍ DỤ THỰC TẾ
# ─────────────
#   # Nạp reference + 5 phim nổi tiếng
#   python run_etl.py --mode all --id 550 238 278 680 13
#
#   # Re-sync chỉ reference data (sau khi TMDb cập nhật)
#   python run_etl.py --mode reference
#
#   # Chạy 1 phim, dừng ngay nếu lỗi
#   python run_etl.py --mode movie --id 550 --stop-on-error
# =============================================================================

import argparse
import logging
import sys
import os

# Đảm bảo import từ cùng thư mục với run_etl.py
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import config
from etl_reference import run_all_reference
from etl_movie import run_movie_etl, run_movies_etl


def setup_logging(debug: bool = False):
    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.StreamHandler(sys.stdout),
        ]
    )


def parse_args():
    parser = argparse.ArgumentParser(
        description="TMDb → PostgreSQL ETL Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ví dụ:
  python run_etl.py --mode reference
  python run_etl.py --mode movie --id 550
  python run_etl.py --mode movie --id 550 278 238
  python run_etl.py --mode all   --id 550 278
        """
    )
    parser.add_argument(
        "--mode",
        choices=["reference", "movie", "all"],
        required=True,
        help=(
            "reference = chỉ nạp lookup tables; "
            "movie = chỉ ETL movie(s); "
            "all = reference + movie"
        )
    )
    parser.add_argument(
        "--id",
        type=int,
        nargs="+",
        metavar="MOVIE_ID",
        help="TMDb movie_id(s), VD: --id 550 278 238"
    )
    parser.add_argument(
        "--stop-on-error",
        action="store_true",
        help="Dừng batch khi gặp lỗi đầu tiên"
    )
    parser.add_argument(
        "--no-log",
        action="store_true",
        help="Tắt ghi ETL_Log vào DB"
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Bật DEBUG logging"
    )
    return parser.parse_args()


def main():
    args = parse_args()
    setup_logging(debug=args.debug)

    # Áp dụng flags
    if args.no_log:
        config.ENABLE_ETL_LOG = False

    logger = logging.getLogger("run_etl")

    # ── Validate ──────────────────────────────────────────────────────────────
    if args.mode in ("movie", "all") and not args.id:
        logger.error("--mode %s yêu cầu --id MOVIE_ID(s)", args.mode)
        sys.exit(1)

    if config.TMDB_API_TOKEN == "YOUR_TMDB_API_READ_ACCESS_TOKEN":
        logger.error(
            "Chưa cấu hình TMDB_API_TOKEN trong config.py! "
            "Vào https://www.themoviedb.org/settings/api để lấy token."
        )
        sys.exit(1)

    # ── Kiểm tra kết nối DB ───────────────────────────────────────────────────
    try:
        from db_utils import get_connection
        conn = get_connection()
        conn.close()
        logger.info("✓ Kết nối PostgreSQL thành công")
    except Exception as e:
        logger.error("✗ Không thể kết nối PostgreSQL: %s", e)
        logger.error("  Kiểm tra lại DB_CONFIG trong config.py")
        sys.exit(1)

    # ── Run ───────────────────────────────────────────────────────────────────
    exit_code = 0

    if args.mode in ("reference", "all"):
        run_all_reference()

    if args.mode in ("movie", "all"):
        movie_ids = args.id
        if len(movie_ids) == 1:
            ok = run_movie_etl(movie_ids[0])
            exit_code = 0 if ok else 1
        else:
            success, failed = run_movies_etl(
                movie_ids,
                stop_on_error=args.stop_on_error
            )
            exit_code = 0 if failed == 0 else 1

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
