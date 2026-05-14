"""
Применить миграции master.person_identity и правила PF-only / очередь сопоставления.

  python scripts/apply_person_identity_migration.py

Файлы: sql/021_master_person_identity.sql, sql/024_master_identity_pf_rules.sql.
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
    paths = [
        _ROOT / "sql" / "021_master_person_identity.sql",
        _ROOT / "sql" / "024_master_identity_pf_rules.sql",
    ]
    for sql_path in paths:
        if not sql_path.is_file():
            raise SystemExit(f"Нет файла: {sql_path}")
    with connect(get_database_url()) as conn:
        for sql_path in paths:
            init_schema(conn, str(sql_path))
            print(f"Применено: {sql_path.name}", flush=True)


if __name__ == "__main__":
    main()
