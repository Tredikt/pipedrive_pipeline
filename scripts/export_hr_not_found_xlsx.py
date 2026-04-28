"""

Один лист Excel: записи из PeopleForce и Pipedrive, которые не привязаны к HR-мастеру

(нет синхронизации с master.hr_employee).



  • PeopleForce: сотрудник есть в peopleforce_dm.employee, но ни у какой строки master нет такого pf_id.

  • Pipedrive: пользователь в pipedrive_dm с непустым email, но ни у какой строки master email не совпадает.



  python scripts/export_hr_not_found_xlsx.py

  python scripts/export_hr_not_found_xlsx.py -o files/hr_not_found.xlsx



Нужны master.hr_employee и витрины peopleforce_dm / pipedrive_dm.

"""



from __future__ import annotations



import argparse

import sys

from pathlib import Path



_ROOT = Path(__file__).resolve().parents[1]

if str(_ROOT) not in sys.path:

    sys.path.insert(0, str(_ROOT))



from dotenv import load_dotenv



load_dotenv()



from openpyxl import Workbook



from src.config import get_database_url

from src.db import connect

from src.excel_openpyxl_value import cell_value_for_openpyxl





EXPORT_SQL = """

SELECT *

FROM (

    SELECT

        'peopleforce'::text AS source_system,

        e.id AS source_id,

        e.full_name AS name,

        e.email,

        e.personal_email AS alt_email

    FROM peopleforce_dm.employee e

    WHERE NOT EXISTS (

        SELECT 1

        FROM master.hr_employee h

        WHERE h.pf_id = e.id

    )



    UNION ALL



    SELECT

        'pipedrive'::text,

        u.id,

        u.name,

        u.email,

        NULL::text

    FROM pipedrive_dm.pipedrive_user u

    WHERE COALESCE(trim(u.email), '') <> ''

      AND NOT EXISTS (

          SELECT 1

          FROM master.hr_employee h

          WHERE lower(trim(h.email)) = lower(trim(u.email))

      )

) t

ORDER BY source_system, source_id

"""






def main() -> None:

    p = argparse.ArgumentParser(

        description="Excel: одна таблица без привязки к master.hr_employee"

    )

    p.add_argument(

        "--output",

        "-o",

        type=Path,

        default=_ROOT / "files" / "hr_not_found.xlsx",

        help="Путь к .xlsx",

    )

    args = p.parse_args()



    args.output.parent.mkdir(parents=True, exist_ok=True)



    from psycopg import errors as pg_errors



    wb = Workbook()

    ws = wb.active

    assert ws is not None

    ws.title = "Без_синхронизации"



    n = 0

    with connect(get_database_url()) as conn:

        conn.autocommit = True

        with conn.cursor() as cur:

            try:

                cur.execute(EXPORT_SQL)

            except pg_errors.UndefinedTable as e:

                raise SystemExit(

                    "Нет таблиц master / peopleforce_dm / pipedrive_dm — "

                    "проверьте миграции и синк."

                ) from e

            desc = cur.description

            if desc:

                ws.append([d.name for d in desc])

                for row in cur:

                    ws.append([cell_value_for_openpyxl(x) for x in row])

                    n += 1



    wb.save(args.output)

    print(f"Записано строк: {n} → {args.output}", flush=True)





if __name__ == "__main__":

    main()


