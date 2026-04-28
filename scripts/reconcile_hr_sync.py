"""

Пересчёт колонок sync_status_* в master.hr_employee по витринам PeopleForce и Pipedrive.



Перед UPDATE автоматически применяется sql/016_master_sync_columns.sql (идемпотентно).

После сверки по возможности применяется sql/017 (представления для экспорта «не найдено»).



  python scripts/reconcile_hr_sync.py



DATABASE_URL — как у остальных скриптов.

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

            parts.append(stmt)

    return parts





def main() -> None:

    sql_016 = _ROOT / "sql" / "016_master_sync_columns.sql"

    sql_017 = _ROOT / "sql" / "017_master_views_reconciliation.sql"

    sql_018 = _ROOT / "sql" / "018_master_reconcile_sync_status.sql"



    if not sql_016.is_file():

        raise SystemExit(f"Нет файла: {sql_016}")

    if not sql_018.is_file():

        raise SystemExit(f"Нет файла: {sql_018}")



    with connect(get_database_url()) as conn:

        with conn.cursor() as cur:

            for stmt in _split_sql(sql_016.read_text(encoding="utf-8")):

                cur.execute(stmt)

            for stmt in _split_sql(sql_018.read_text(encoding="utf-8")):

                cur.execute(stmt)

        conn.commit()

    print("sync_status_* пересчитаны.", flush=True)



    if sql_017.is_file():

        try:

            with connect(get_database_url()) as conn:

                with conn.cursor() as cur:

                    for stmt in _split_sql(sql_017.read_text(encoding="utf-8")):

                        cur.execute(stmt)

                conn.commit()

            print("Представления master.v_pf_* / v_pipedrive_* обновлены (017).", flush=True)

        except Exception as ex:

            print(

                "017 не применён (нужны схемы peopleforce_dm / pipedrive_dm и master):",

                ex,

                flush=True,

            )





if __name__ == "__main__":

    main()


