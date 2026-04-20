"""Удаление строк витрины и raw при webhook delete."""

from __future__ import annotations

from typing import Any

from src.tz_structure import ENTITY_DM_TABLE
from src.webhook_mapping import SPEC_TO_CF_ENTITY


def delete_entity_from_db(conn: Any, spec_name: str, pipedrive_id: str) -> None:
    """Удалить сущность из pipedrive_dm, custom_field_value, entity_record."""
    cf_type = SPEC_TO_CF_ENTITY.get(spec_name)
    dm_table = ENTITY_DM_TABLE.get(spec_name)

    with conn.cursor() as cur:
        if cf_type:
            cur.execute(
                """
                DELETE FROM pipedrive_dm.custom_field_value
                WHERE entity_type = %s AND entity_id = %s
                """,
                (cf_type, pipedrive_id),
            )
        cur.execute(
            """
            DELETE FROM pipedrive_raw.entity_record
            WHERE entity_type = %s AND pipedrive_id = %s
            """,
            (spec_name, pipedrive_id),
        )
        if dm_table:
            if spec_name == "leads":
                cur.execute(
                    f'DELETE FROM pipedrive_dm."{dm_table}" WHERE id = %s',
                    (pipedrive_id,),
                )
            else:
                try:
                    iid = int(pipedrive_id)
                except ValueError:
                    iid = pipedrive_id
                cur.execute(
                    f'DELETE FROM pipedrive_dm."{dm_table}" WHERE id = %s',
                    (iid,),
                )
