"""
Сверка списка ожидаемых полей (из ТЗ/CRM) с pipedrive_raw.field_definition.

Имена в Pipedrive должны совпасть с выгруженными field_name (как в API *Fields).
Кастомные значения лежат в pipedrive_dm.custom_field_value; стандартные колонки — в таблицах витрины
(например person.job_title может дублировать «Position»).

Запуск: python scripts/check_field_checklist.py
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from dotenv import load_dotenv

load_dotenv()

import psycopg

# Ожидаемые подписи (как у вас в документе) → entity_type в field_definition
CHECKLIST: dict[str, tuple[str, ...]] = {
    "deals": (
        "Leadsource",
        "Intro Meeting",
        "Decision makers (names and roles)",
        "Decision Factors",
        "Competitor",
        "Project Budget",
        "Network launch date",
        "Start date of Network launch date",
        "End date of Network launch date",
        "What are their alternatives?",
        "Sales strategy",
        "Typeform before the meeting sent",
        "Company presentation shown",
        "Script questions asked",
        "Software demo done",
        "Blockers",
        "Project manager",
        "Webinar",
        "Webinar Attendance",
        "Types of cooperation (Business Model)",
        "Client's pains / needs",
        "Interest level",
        "Category (Number of Lockers)",
        "Category (Relationship)",
        "Industry (Apollo)",
        "Annual Parcel Volume",
    ),
    "leads": (
        "Leadsource",
        "Intro meeting",
        "Decision makers (names and roles)",
        "Decision Factors",
        "Competitor",
        "Project Budget",
        "Network launch date",
        "End date of Network launch date",
        "What are their alternatives?",
        "Sales strategy",
        "Typeform before the meeting sent",
        "Company presentation shown",
        "Script questions asked",
        "Project manager",
        "Software demo done",
        "Blockers",
        "Webinar",
        "Webinar Attendance",
        "Types of cooperation (Business Model)",
        "Client's pains / needs",
        "Interest level",
        "Category (Number of Lockers)",
        "Industry (Apollo)",
        "Annual Parcel Volume",
    ),
    "organizations": (
        "Industry",
        "Company figures: turnover, number of employees, warehouse volumes",
        "Contact Source",
        "Country",
        "PR activities: articles, posts about the company",
        "Experience in self-service",
        "Product of interest",
        "Reason to buy",
        "Company - Category (Number of Lockers)",
        "Company - Category (Relationship)",
        "Apollo (Industry)",
        "Total Funding",
        "Phone (Company)",
        "LinkedIn URL",
    ),
    "persons": (
        "Position",
        "Linkedin",
        "How many years work in the company",
        "Additional phone / email",
        "Personal charakteristics",
        "Feedback",
        "Links in media",
        "Other phone number",
    ),
    "products": ("Comment",),
}


def _norm(s: str) -> str:
    return re.sub(r"[^a-z0-9]", "", s.lower())


def _load_catalog(cur: psycopg.Cursor) -> dict[str, list[str]]:
    cur.execute(
        """
        SELECT entity_type, field_name
        FROM pipedrive_raw.field_definition
        ORDER BY entity_type, field_name
        """
    )
    out: dict[str, list[str]] = {}
    for et, fn in cur.fetchall():
        out.setdefault(et, []).append(fn)
    return out


def _match(expected: str, catalog_names: list[str]) -> str | None:
    ne = _norm(expected)
    if not ne:
        return None
    best: str | None = None
    for name in catalog_names:
        nn = _norm(name)
        if ne == nn or ne in nn or nn in ne:
            best = name
            break
    return best


def main() -> None:
    import os

    db = os.environ.get("DATABASE_URL", "").strip()
    if not db:
        raise SystemExit("DATABASE_URL не задан")

    print("Каталог полей: pipedrive_raw.field_definition (после синка *Fields).\n")
    with psycopg.connect(db) as conn:
        with conn.cursor() as cur:
            catalog = _load_catalog(cur)
            for et, expected_list in CHECKLIST.items():
                names = catalog.get(et, [])
                print("=" * 72)
                print(f"{et.upper()}  — в каталоге полей: {len(names)}")
                if not names:
                    print("  (пусто: нет строк в field_definition — прогоните синк сделок/лидов/…)")
                missing: list[str] = []
                for exp in expected_list:
                    m = _match(exp, names)
                    if m:
                        print(f"  OK  «{exp}»  →  в API: «{m}»")
                    else:
                        missing.append(exp)
                        print(f"  ??  «{exp}»  — нет совпадения в field_definition")
                if missing:
                    print(f"\n  Итого не найдено по грубому сравнению: {len(missing)}")
                print()

    print("Примечания:")
    print("  • Подписи в Pipedrive могут отличаться на1 символ — смотрите список выше и правьте CHECKLIST.")
    print("  • «Position» у контакта может быть стандартным job_title в pipedrive_dm.person, не кастомом.")
    print("  • В витрине EAV: custom_field_value.field_name — sanitized; в v_custom_fields_labeled см. pipedrive_field_name (после sql/008).")


if __name__ == "__main__":
    main()
