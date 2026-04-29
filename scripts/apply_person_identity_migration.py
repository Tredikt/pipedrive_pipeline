"""
Применить sql/021_master_person_identity.sql (таблица master.person_identity).

  python scripts/apply_person_identity_migration.py

Нужен DATABASE_URL (или в .env).
"""

from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from dotenv import load_dotenv

load_dotenv()

from src.config import get_database_url
from src.db import connect, init_schema


def main() -> None:
    sql_path = _ROOT / "sql" / "021_master_person_identity.sql"
    if not sql_path.is_file():
        raise SystemExit(f"Нет файла: {sql_path}")
    with connect(get_database_url()) as conn:
        init_schema(conn, str(sql_path))
    print(f"Применено: {sql_path}", flush=True)


if __name__ == "__main__":
    main()
