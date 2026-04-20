from __future__ import annotations

import re
from typing import Any

# Ключи кастомных полей в теле сущности (обычно 40 hex-символов из personFields и т.д.)
_CUSTOM_FIELD_KEY_RE = re.compile(r"^[0-9a-fA-F]{32,40}$")


def _is_likely_custom_field_key(key: str) -> bool:
    return bool(key and _CUSTOM_FIELD_KEY_RE.match(key))


def _sanitize_label(name: str) -> str:
    s = name.strip() or "unnamed_field"
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"[^0-9a-zA-Z_]", "", s)
    if s and s[0].isdigit():
        s = f"f_{s}"
    return s[:200] or "field"


def build_field_key_to_label(field_rows: list[dict[str, Any]]) -> dict[str, str]:
    """key API -> безопасное человекочитаемое имя для JSON-колонки."""
    mapping: dict[str, str] = {}
    used: set[str] = set()
    for row in field_rows:
        key = str(row.get("key") or "")
        if not key:
            continue
        base = _sanitize_label(str(row.get("name") or key))
        label = base
        n = 2
        while label in used:
            label = f"{base}_{n}"
            n += 1
        used.add(label)
        mapping[key] = label
    return mapping


def extract_custom_resolved(
    record: dict[str, Any],
    field_key_to_label: dict[str, str],
    *,
    skip_keys: frozenset[str],
) -> dict[str, Any]:
    """Кастомные поля по *Fields: все объявленные ключи, в т.ч. отсутствующие в record и null."""
    out: dict[str, Any] = {}
    for api_key, label in field_key_to_label.items():
        if api_key in skip_keys:
            continue
        out[label] = record.get(api_key)

    for api_key, val in record.items():
        if api_key in skip_keys or api_key in field_key_to_label:
            continue
        if _is_likely_custom_field_key(api_key):
            out[api_key] = val

    return out


def iter_custom_field_rows(
    record: dict[str, Any],
    field_key_to_label: dict[str, str],
    *,
    skip_keys: frozenset[str],
) -> list[tuple[str, str, Any]]:
    """Для pipedrive_dm.custom_field_value: (api_key, human_name, value); null/пустые тоже пишем."""
    rows: list[tuple[str, str, Any]] = []
    seen: set[str] = set()

    for api_key, label in field_key_to_label.items():
        if api_key in skip_keys:
            continue
        seen.add(api_key)
        rows.append((api_key, label, record.get(api_key)))

    for api_key, val in record.items():
        if api_key in skip_keys or api_key in seen:
            continue
        if not _is_likely_custom_field_key(api_key):
            continue
        seen.add(api_key)
        rows.append((api_key, api_key, val))

    rows.sort(key=lambda t: (t[1], t[0]))
    return rows


def standard_skip_keys(entity_name: str) -> frozenset[str]:
    # Стандартные поля сущностей Pipedrive; кастомные — обычно hex-подобные ключи из field defs
    common = {
        "id",
        "active_flag",
        "add_time",
        "update_time",
        "visible_to",
        "owner_id",
    }
    if entity_name == "deals":
        return frozenset(
            common
            | {
                "title",
                "value",
                "currency",
                "stage_id",
                "pipeline_id",
                "person_id",
                "org_id",
                "status",
                "probability",
                "lost_reason",
                "expected_close_date",
                "label",
            }
        )
    if entity_name == "persons":
        return frozenset(
            common
            | {
                "name",
                "first_name",
                "last_name",
                "email",
                "phone",
                "im",
                "org_id",
                "primary_email",
                "org_name",
                "owner_name",
                "cc_email",
                "job_title",
                "birthday",
                "notes",
                "label",
                "label_ids",
                "picture_id",
                "first_char",
                "company_id",
                "files_count",
                "notes_count",
                "followers_count",
                "won_deals_count",
                "lost_deals_count",
                "open_deals_count",
                "closed_deals_count",
                "related_won_deals_count",
                "related_lost_deals_count",
                "related_open_deals_count",
                "related_closed_deals_count",
                "participant_open_deals_count",
                "participant_closed_deals_count",
                "done_activities_count",
                "undone_activities_count",
                "activities_count",
                "email_messages_count",
                "last_activity_id",
                "next_activity_id",
                "last_activity_date",
                "next_activity_date",
                "next_activity_time",
                "last_incoming_mail_time",
                "last_outgoing_mail_time",
                "postal_address",
                "postal_address_lat",
                "postal_address_long",
                "postal_address_route",
                "postal_address_country",
                "postal_address_locality",
                "postal_address_subpremise",
                "postal_address_postal_code",
                "postal_address_sublocality",
                "postal_address_street_number",
                "postal_address_formatted_address",
                "postal_address_admin_area_level_1",
                "postal_address_admin_area_level_2",
                "delete_time",
            }
        )
    if entity_name == "organizations":
        return frozenset(
            common
            | {
                "name",
                "address",
                "owner_id",
                "label",
                "label_ids",
                "cc_email",
                "people_count",
                "company_id",
                "org_name",
                "owner_name",
                "next_activity_id",
                "last_activity_id",
                "last_activity_date",
                "next_activity_date",
                "open_deals_count",
                "related_open_deals_count",
                "employee_count",
                "picture_id",
                "website",
                "phone",
                "delete_time",
            }
        )
    if entity_name == "activities":
        return frozenset(
            common
            | {
                "subject",
                "type",
                "due_date",
                "due_time",
                "duration",
                "deal_id",
                "person_id",
                "org_id",
                "note",
                "user_id",
                "group_id",
                "company_id",
                "busy_flag",
                "public_description",
                "done",
                "location",
                "marked_as_done_time",
                "type_name",
                "recurring_rule",
                "conference_meeting_client",
                "conference_meeting_url",
                "attendees",
            }
        )
    if entity_name == "leads":
        return frozenset(
            common
            | {
                "title",
                "person_id",
                "organization_id",
                "org_id",
                "value",
                "currency",
                "pipeline_id",
                "stage_id",
                "source_name",
                "expected_close_date",
                "label_ids",
                "archived",
                "was_seen",
                "creator_user_id",
                "is_archived",
                "visible_to",
                "cc_email",
                "company_id",
            }
        )
    if entity_name == "products":
        return frozenset(
            common
            | {
                "name",
                "code",
                "unit",
                "tax",
                "category",
                "owner_id",
                "prices",
                "selectable",
                "first_char",
                "visible_to",
                "company_id",
                "direct_price",
                "product_category",
            }
        )
    if entity_name == "notes":
        return frozenset(
            common
            | {
                "content",
                "deal_id",
                "person_id",
                "org_id",
                "lead_id",
                "project_id",
                "add_user_id",
                "update_user_id",
                "user_id",
                "pinned_to_deal_flag",
                "pinned_to_organization_flag",
                "pinned_to_person_flag",
                "deal",
                "person",
                "org",
                "lead",
            }
        )
    if entity_name == "call_logs":
        return frozenset(
            common
            | {
                "activity_id",
                "deal_id",
                "person_id",
                "org_id",
                "lead_id",
                "user_id",
                "subject",
                "duration",
                "outcome",
                "from_phone_number",
                "to_phone_number",
                "start_time",
                "end_time",
                "note",
                "company_id",
            }
        )
    if entity_name == "files":
        return frozenset(
            common
            | {
                "name",
                "file_name",
                "file_type",
                "file_size",
                "remote_location",
                "file_remote_location",
                "s3_bucket",
                "url",
                "deal_id",
                "person_id",
                "org_id",
                "product_id",
                "lead_id",
                "activity_id",
                "project_id",
                "mail_message_id",
                "log_id",
                "user_id",
                "add_user_id",
                "cid",
                "company_id",
            }
        )
    if entity_name == "projects":
        return frozenset(
            common
            | {
                "title",
                "name",
                "status",
                "pipeline_id",
                "phase_id",
                "start_date",
                "end_date",
                "description",
                "board_id",
                "company_id",
            }
        )
    if entity_name == "users":
        return frozenset(
            {
                "id",
                "name",
                "email",
                "phone",
                "lang",
                "locale",
                "timezone_name",
                "timezone_offset",
                "activated",
                "last_login",
                "created",
                "modified",
                "signup_flow_variation",
                "has_created_company",
                "is_admin",
                "active_flag",
                "role_id",
                "default_currency",
                "icon_url",
                "item_display_id",
                "score",
                "is_you",
                "created_company_id",
                "has_pic",
                "pic_hash",
                "access",
                "role_id_admin",
            }
        )
    return frozenset(common)
