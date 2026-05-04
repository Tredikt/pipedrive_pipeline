"""Проверка DATABASE_URL из корневого .env (без вывода пароля).

  python scripts/check_database_connection.py

Опционально: таймаут подключения (--timeout), другой env-файл (--env-file).
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from urllib.parse import urlparse

from dotenv import load_dotenv

_ROOT = Path(__file__).resolve().parents[1]


def _target_label(url: str) -> str:
    p = urlparse(url)
    port = p.port
    if port is None and (p.scheme or "").startswith("postgres"):
        port = 5432
    host = p.hostname or "?"
    db = (p.path or "/").strip("/") or "?"
    return f"{p.scheme}://{host}:{port}/{db}"


def main() -> None:
    ap = argparse.ArgumentParser(description="Ping PostgreSQL по DATABASE_URL.")
    ap.add_argument(
        "--env-file",
        type=Path,
        default=_ROOT / ".env",
        help="Файл с переменными (по умолчанию корневой .env)",
    )
    ap.add_argument(
        "--timeout",
        type=float,
        default=15.0,
        help="connect_timeout, секунды (по умолчанию 15)",
    )
    args = ap.parse_args()

    load_dotenv(args.env_file)
    url = os.environ.get("DATABASE_URL", "").strip()
    if not url:
        raise SystemExit("DATABASE_URL не задан (проверьте .env).")

    print("Target:", _target_label(url))

    try:
        import psycopg
    except ImportError as e:
        raise SystemExit(
            "Нужен psycopg: pip install 'psycopg[binary]'"
        ) from e

    try:
        with psycopg.connect(url, connect_timeout=int(args.timeout)) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.execute(
                    "SELECT current_database(), current_user, "
                    "inet_server_addr(), inet_server_port()"
                )
                dbname, user, srv_host, srv_port = cur.fetchone()
                cur.execute("SELECT version()")
                version_line = cur.fetchone()[0].split("\n", 1)[0]
    except Exception as e:
        print("Connection: FAILED —", type(e).__name__ + ":", e)
        raise SystemExit(1) from e

    print("Connection: OK")
    print(f"  database={dbname!r} user={user!r} server={srv_host}:{srv_port}")
    print(f"  {version_line}")


if __name__ == "__main__":
    main()
