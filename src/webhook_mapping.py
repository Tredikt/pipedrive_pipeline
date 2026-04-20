"""Соответствие meta.entity из Pipedrive Webhooks v2 → имя сущности в ENTITY_SPECS."""

from __future__ import annotations

# v2: meta.entity часто в единственном числе
# https://pipedrive.readme.io/docs/guide-for-webhooks-v2
WEBHOOK_ENTITY_TO_SPEC: dict[str, str] = {
    "deal": "deals",
    "person": "persons",
    "organization": "organizations",
    "lead": "leads",
    "activity": "activities",
    "product": "products",
    "note": "notes",
    "pipeline": "pipelines",
    "stage": "stages",
    "user": "users",
    "project": "projects",
    "file": "files",
    "call_log": "call_logs",
    # дубликаты на случай другого регистра/форм
    "deal_product": "deal_products",
}


def spec_name_from_webhook_entity(entity: str | None) -> str | None:
    if not entity:
        return None
    e = entity.strip().lower()
    if e in WEBHOOK_ENTITY_TO_SPEC:
        return WEBHOOK_ENTITY_TO_SPEC[e]
    # уже plural как в коде
    for s in WEBHOOK_ENTITY_TO_SPEC.values():
        if e == s:
            return s
    return None


# custom_field_value.entity_type (единственное число)
SPEC_TO_CF_ENTITY: dict[str, str] = {
    "persons": "person",
    "organizations": "organization",
    "deals": "deal",
    "activities": "activity",
    "leads": "lead",
    "products": "product",
    "notes": "note",
    "call_logs": "call_log",
    "projects": "project",
}
