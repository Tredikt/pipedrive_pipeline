"""Разбор тел Pipedrive webhook v2: create / change / delete."""

from __future__ import annotations

import pytest

from src.webhook_parse import (
    is_delete_action,
    is_upsert_action,
    parse_webhook_event,
    row_from_webhook_body,
)


def _meta(
    action: str,
    entity_id: int,
    *,
    host: str = "acme.pipedrive.com",
) -> dict:
    return {
        "action": action,
        "entity": "deal",
        "entity_id": entity_id,
        "version": "2.0",
        "host": host,
    }


@pytest.mark.parametrize("action", ("create", "change", "delete"))
def test_parse_create_change_delete(action: str) -> None:
    body: dict = {"meta": _meta(action, 7323), "data": {"id": 7323, "title": "t"}}
    parsed = parse_webhook_event(body)
    assert parsed == (action, "deals", "7323")


def test_is_delete_action_variants():
    assert is_delete_action("delete")
    assert is_delete_action("deleted")
    assert not is_delete_action("change")


def test_is_upsert_covers_create_change():
    assert is_upsert_action("create")
    assert is_upsert_action("change")
    assert not is_upsert_action("delete")


def test_row_from_webhook_body_matches_entity_id():
    body = {
        "meta": _meta("change", 99),
        "data": {"id": 99, "title": "from webhook", "custom_fields": {"abc": 1}},
    }
    row = row_from_webhook_body(body, "99")
    assert row is not None
    assert row["title"] == "from webhook"
    assert row["abc"] == 1
