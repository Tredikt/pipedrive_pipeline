"""Разбор тела webhook Pipedrive v1 / v2."""

from __future__ import annotations

import logging
import uuid
from typing import Any

from src.webhook_mapping import spec_name_from_webhook_entity

logger = logging.getLogger(__name__)


def _normalize_entity_id_for_compare(raw: Any) -> str:
    """Одинаковые UUID/hyphens/meta vs data; числовые id — без .0."""
    if raw is None:
        return ""
    s = str(raw).strip()
    if len(s) >= 2 and s.endswith(".0") and s[:-2].replace("-", "").isdigit():
        s = s[:-2]
    t = s.replace(" ", "")
    if len(t) in (32, 36) and all(c in "0123456789abcdefABCDEF-" for c in t):
        try:
            return str(uuid.UUID(t))
        except ValueError:
            pass
    return s.lower()


def merge_api_row_with_webhook(
    api_row: dict[str, Any] | None,
    webhook_row: dict[str, Any] | None,
) -> dict[str, Any] | None:
    """
    GET даёт полный объект; webhook часто — только изменённые поля.
    Не-null поля из webhook перекрывают GET (иначе COALESCE в UPSERT «замораживает» старые значения).
    """
    if api_row is None and webhook_row is None:
        return None
    if api_row is None:
        return dict(webhook_row) if webhook_row else None
    if webhook_row is None:
        return dict(api_row)
    out = dict(api_row)
    for k, v in webhook_row.items():
        if v is not None:
            out[k] = v
    return out


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


def _data_matches_entity(data: dict[str, Any], entity_id: str) -> bool:
    want = _normalize_entity_id_for_compare(entity_id)
    for k in ("id", "uuid", "lead_id", "file_id"):
        v = data.get(k)
        if v is not None and _normalize_entity_id_for_compare(v) == want:
            return True
    return False


def row_from_webhook_body(body: dict[str, Any], entity_id: str) -> dict[str, Any] | None:
    """
    Webhooks v2: в теле есть meta + data (текущее состояние объекта).
    Если data совпадает с entity_id — приводим к форме, близкой к GET /v1/.../{id},
    чтобы не делать повторный GET (например при 403 у токена на сервере).

    См. https://pipedrive.readme.io/docs/guide-for-webhooks-v2
    """
    data = body.get("data")
    if not isinstance(data, dict):
        return None
    if not _data_matches_entity(data, entity_id):
        logger.warning(
            "webhook data id не совпал с meta.entity_id: meta=%r data.keys=%s",
            entity_id,
            list(data.keys())[:25],
        )
        return None
    out = dict(data)
    if out.get("id") is None:
        for k in ("uuid", "lead_id"):
            if out.get(k) is not None:
                out["id"] = out[k]
                break
        if out.get("id") is None:
            out["id"] = entity_id
    # v2: кастомные поля могут быть в custom_fields — плоский hash, как в API
    cf = out.pop("custom_fields", None)
    if isinstance(cf, dict):
        out.update(cf)
    return out


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
