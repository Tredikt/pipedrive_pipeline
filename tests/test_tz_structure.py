"""Структурные тесты без вызова Pipedrive: маппинг сущностей, наличие SQL-файлов."""
from __future__ import annotations

import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class TestTzStructure(unittest.TestCase):
    def test_dm_table_mapping_matches_sync(self) -> None:
        from src.sync import DM_ENTITIES
        from src.tz_structure import ENTITY_DM_TABLE

        self.assertEqual(
            set(ENTITY_DM_TABLE.keys()),
            set(DM_ENTITIES),
            "Каждая DM-сущность должна иметь таблицу в ENTITY_DM_TABLE и наоборот",
        )

    def test_migration_files_exist(self) -> None:
        from src.tz_structure import SQL_MIGRATION_FILES

        sql_dir = ROOT / "sql"
        for name in SQL_MIGRATION_FILES:
            self.assertTrue(
                (sql_dir / name).is_file(),
                f"Отсутствует {sql_dir / name}",
            )

    def test_entity_specs_unique_names(self) -> None:
        from src.entities import ENTITY_SPECS

        names = [s.name for s in ENTITY_SPECS]
        self.assertEqual(len(names), len(set(names)), "Дубли имён в ENTITY_SPECS")


if __name__ == "__main__":
    unittest.main()
