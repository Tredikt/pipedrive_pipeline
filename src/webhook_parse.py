"""Разбор тела webhook Pipedrive v1 / v2."""

from __future__ import annotations

from typing import Any

from src.webhook_mapping import spec_name_from_webhook_entity


def parse_webhook_event(body: dict[str, Any]) -> tuple[str, str, str] | None:
    """
    Возвращает (action, spec_name, entity_id) или None, если не обрабатываем.
    action: create | change | delete | ... (нормализуем ниже)
    """
    meta = body.get("meta")
    if isinstance(meta, dict):
        action = str(meta.get("action") or meta.get("event") or "").lower()
        entity = meta.get("entity") or meta.get("object")
        eid = meta.get("entity_id")
        if eid is None:
            eid = meta.get("id")
        if entity is not None and eid is not None:
            spec = spec_name_from_webhook_entity(str(entity))
            if spec:
                return (action or "change", spec, str(eid))

    # v1: иногда meta на верхнем уровне без вложенности так же
    if isinstance(meta, dict) and meta.get("v") == 1:
        action = str(meta.get("action") or "").lower()
        obj = meta.get("object")
        eid = meta.get("id")
        if obj and eid is not None:
            spec = spec_name_from_webhook_entity(str(obj))
            if spec:
                return (action or "change", spec, str(eid))

    return None


def is_delete_action(action: str) -> bool:
    a = action.lower()
    return a in ("delete", "deleted", "remove", "removed")

def is_upsert_action(action: str) -> bool:
    a = action.lower()
    return a in (
        "create",
        "change",
        "update",
        "added",
        "updated",
        "merged",
        "*",
        "",
    )
