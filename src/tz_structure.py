"""Маппинг сущностей синка → таблицы витрины (аудит структуры / ТЗ)."""

from __future__ import annotations

# Имя сущности (как в ENTITY_SPECS / entity_record.entity_type) → таблица pipedrive_dm
ENTITY_DM_TABLE: dict[str, str] = {
    "persons": "person",
    "organizations": "organization",
    "deals": "deal",
    "activities": "activity",
    "leads": "lead",
    "products": "product",
    "notes": "note",
    "call_logs": "call_log",
    "files": "file",
    "projects": "project",
    "users": "pipedrive_user",
    "pipelines": "pipeline",
    "stages": "stage",
    "currencies": "currency",
    "deal_products": "deal_product_line",
}

REQUIRED_RAW_TABLES: tuple[tuple[str, str], ...] = (
    ("pipedrive_raw", "field_definition"),
    ("pipedrive_raw", "entity_record"),
)

REQUIRED_DM_VIEWS: tuple[tuple[str, str], ...] = (
    ("pipedrive_dm", "v_custom_fields_labeled"),
    ("pipedrive_dm", "v_phone_calls_from_activities"),
)

SQL_MIGRATION_FILES: tuple[str, ...] = (
    "001_init_schema.sql",
    "002_pipedrive_dm.sql",
    "003_dm_crm_core_entities.sql",
    "004_dm_custom_fields_labeled_view.sql",
    "006_view_phone_calls_from_activities.sql",
    "007_dm_reference_and_deal_products.sql",
    "008_v_custom_fields_pipedrive_name.sql",
)
