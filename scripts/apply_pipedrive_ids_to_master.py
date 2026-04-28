"""
Заполнить master.hr_employee.pipedrive_user_id и pipedrive_person_id по email из pipedrive_dm.

Использует sql/019_master_fill_pipedrive_ids_from_dm.sql (та же логика, что и matched в Excel).

  python scripts/apply_pipedrive_ids_to_master.py

Нужны DATABASE_URL и актуальная витрина pipedrive_dm.
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
from src.db import connect


def _split_sql(sql: str) -> list[str]:
    parts = []
    for raw in sql.split(";"):
        stmt = raw.strip()
        if stmt:
            parts.append(stmt + ";")
    return parts


def main() -> None:
    sql_path = _ROOT / "sql" / "019_master_fill_pipedrive_ids_from_dm.sql"
    if not sql_path.is_file():
        raise SystemExit(f"Нет файла: {sql_path}")

    stmts = _split_sql(sql_path.read_text(encoding="utf-8"))

    with connect(get_database_url()) as conn:
        with conn.cursor() as cur:
            for stmt in stmts:
                cur.execute(stmt)
        conn.commit()

    print(
        "Обновлены pipedrive_user_id и pipedrive_person_id в master.hr_employee по email.",
        flush=True,
    )


if __name__ == "__main__":
    main()
