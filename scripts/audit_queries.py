"""Разовая проверка БД: структура и объёмы. Запуск: python scripts/audit_queries.py"""
from __future__ import annotations

import os

from dotenv import load_dotenv

load_dotenv()

import psycopg

URL = os.environ["DATABASE_URL"]

QUERIES: list[tuple[str, str]] = [
    (
        "1. Схемы pipedrive*",
        """
        SELECT schema_name
        FROM information_schema.schemata
        WHERE schema_name LIKE 'pipedrive%'
        ORDER BY 1
        """,
    ),
    (
        "2. Таблицы pipedrive_dm / pipedrive_raw",
        """
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_schema IN ('pipedrive_dm', 'pipedrive_raw')
          AND table_type = 'BASE TABLE'
        ORDER BY 1, 2
        """,
    ),
    (
        "3. Представления pipedrive_dm",
        """
        SELECT table_name
        FROM information_schema.views
        WHERE table_schema = 'pipedrive_dm'
        ORDER BY 1
        """,
    ),
    (
        "4. Оценка строк (pg_stat) — pipedrive_dm",
        """
        SELECT relname AS table_name, n_live_tup::bigint AS est_rows
        FROM pg_stat_user_tables
        WHERE schemaname = 'pipedrive_dm'
        ORDER BY n_live_tup DESC NULLS LAST
        """,
    ),
    (
        "5. Точные COUNT — ключевые таблицы DM",
        """
        SELECT 'person' AS t, COUNT(*)::bigint FROM pipedrive_dm.person
        UNION ALL SELECT 'deal', COUNT(*)::bigint FROM pipedrive_dm.deal
        UNION ALL SELECT 'organization', COUNT(*)::bigint FROM pipedrive_dm.organization
        UNION ALL SELECT 'activity', COUNT(*)::bigint FROM pipedrive_dm.activity
        UNION ALL SELECT 'lead', COUNT(*)::bigint FROM pipedrive_dm.lead
        UNION ALL SELECT 'note', COUNT(*)::bigint FROM pipedrive_dm.note
        UNION ALL SELECT 'product', COUNT(*)::bigint FROM pipedrive_dm.product
        UNION ALL SELECT 'file', COUNT(*)::bigint FROM pipedrive_dm.file
        UNION ALL SELECT 'call_log', COUNT(*)::bigint FROM pipedrive_dm.call_log
        UNION ALL SELECT 'project', COUNT(*)::bigint FROM pipedrive_dm.project
        UNION ALL SELECT 'custom_field_value', COUNT(*)::bigint FROM pipedrive_dm.custom_field_value
        UNION ALL SELECT 'pipedrive_user', COUNT(*)::bigint FROM pipedrive_dm.pipedrive_user
        ORDER BY 2 DESC
        """,
    ),
    (
        "6. Колонки pipedrive_dm.person (фрагмент)",
        """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = 'pipedrive_dm' AND table_name = 'person'
        ORDER BY ordinal_position
        LIMIT 25
        """,
    ),
    (
        "7. Колонки pipedrive_dm.custom_field_value",
        """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = 'pipedrive_dm' AND table_name = 'custom_field_value'
        ORDER BY ordinal_position
        """,
    ),
    (
        "8. Пример: первая персона + несколько кастомных полей",
        """
        SELECT p.id, p.name, p.primary_email, c.field_name,
               LEFT(COALESCE(c.value_text, c.value_json::text), 80) AS value_preview
        FROM pipedrive_dm.person p
        JOIN pipedrive_dm.custom_field_value c
          ON c.entity_type = 'person' AND c.entity_id = p.id::text
        WHERE p.id = (SELECT id FROM pipedrive_dm.person ORDER BY id LIMIT 1)
        ORDER BY c.field_name
        LIMIT 12
        """,
    ),
    (
        "9. pipedrive_raw: field_definition и entity_record (если схема есть)",
        """
        SELECT 'field_definition' AS t, COUNT(*)::bigint FROM pipedrive_raw.field_definition
        UNION ALL
        SELECT 'entity_record', COUNT(*)::bigint FROM pipedrive_raw.entity_record
        """,
    ),
]


def main() -> None:
    with psycopg.connect(URL) as conn:
        for title, sql in QUERIES:
            print("\n" + "=" * 72)
            print(title)
            print("-" * 72)
            print(sql.strip())
            print("-" * 72)
            try:
                with conn.cursor() as cur:
                    cur.execute(sql)
                    cols = [d[0] for d in cur.description] if cur.description else []
                    if cols:
                        print(" | ".join(cols))
                    for row in cur.fetchall():
                        print(" | ".join("" if v is None else str(v) for v in row))
            except Exception as e:
                print(f"[ошибка] {e}")


if __name__ == "__main__":
    main()
