"""
PeopleForce: POST /peopleforce/webhook — create / update (upsert) / delete сотрудника.

Док: action `employee_create` и т.д., подпись `x-peopleforce-signature` (HMAC-SHA256).
https://developer.peopleforce.io/docs/starting-with-webhooks
"""
from __future__ import annotations

import hashlib
import hmac
import json
import os
from collections.abc import Generator
from contextlib import contextmanager
from typing import Any
from unittest.mock import patch

from fastapi.testclient import TestClient

# DATABASE_URL требуется get_database_url() внутри обработчика
os.environ.setdefault("DATABASE_URL", "postgresql://test:test@localhost:5432/test")

from src.webhook_app import app

_pf_client = TestClient(app)


class _StubCursor:
    def __init__(self, log: list[tuple[str, Any]]) -> None:
        self._log = log

    def __enter__(self) -> _StubCursor:
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def execute(self, sql: str, params: Any = None) -> None:
        self._log.append((sql, params))


class _StubConn:
    def __init__(self, log: list[tuple[str, Any]]) -> None:
        self._log = log

    def __enter__(self) -> _StubConn:
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def cursor(self) -> _StubCursor:
        return _StubCursor(self._log)

    def commit(self) -> None:
        pass


@contextmanager
def _fake_connect(_url: str) -> Generator[_StubConn, None, None]:
    log: list[tuple[str, Any]] = []
    yield _StubConn(log)


def _sample_employee_data(eid: int) -> dict[str, Any]:
    return {
        "id": eid,
        "attributes": {
            "first_name": "John",
            "last_name": "Doe",
            "email": "john.peopleforce.test@example.com",
            "employee_number": "PF999001",
        },
        "position": {"id": 10, "name": "Engineer"},
        "department": {"id": 20, "name": "IT"},
        "division": {"id": 30, "name": "EMEA"},
        "location": {"id": 40, "name": "Berlin"},
        "job_level": {"id": 50, "name": "L3"},
        "employment_type": {"id": 60, "name": "Full-time"},
    }


@patch("src.peopleforce.webhook_routes.connect", _fake_connect)
def test_peopleforce_webhook_employee_create() -> None:
    body = {
        "action": "employee_create",
        "data": _sample_employee_data(130333),
    }
    r = _pf_client.post("/peopleforce/webhook", json=body)
    assert r.status_code == 200
    out = r.json()
    assert out["ok"] is True
    assert out.get("action") == "upserted"
    assert out.get("entity") == "employee"
    assert out.get("id") == 130333


@patch("src.peopleforce.webhook_routes.connect", _fake_connect)
def test_peopleforce_webhook_employee_update() -> None:
    body = {
        "action": "employee_update",
        "data": _sample_employee_data(200001),
    }
    r = _pf_client.post("/peopleforce/webhook", json=body)
    assert r.status_code == 200
    j = r.json()
    assert j["ok"] is True
    assert j.get("action") == "upserted"
    assert j["id"] == 200001


@patch("src.peopleforce.webhook_routes.connect", _fake_connect)
def test_peopleforce_webhook_employee_delete() -> None:
    body = {
        "action": "employee_delete",
        "data": {"id": 777,
                 },
    }
    r = _pf_client.post("/peopleforce/webhook", json=body)
    assert r.status_code == 200
    j = r.json()
    assert j["ok"] is True
    assert j.get("action") == "deleted"
    assert j["id"] == 777


@patch("src.peopleforce.webhook_routes.connect", _fake_connect)
def test_peopleforce_webhook_invalid_signature() -> None:
    os.environ["PEOPLEFORCE_WEBHOOK_SECRET"] = "unit-test-secret-123"
    try:
        raw = b'{"action":"employee_create","data":{"id":1,"attributes":{}}}'
        bad_sig = "sha256=0" * 32
        r = _pf_client.post(
            "/peopleforce/webhook",
            content=raw,
            headers={
                "content-type": "application/json",
                "x-peopleforce-signature": bad_sig,
            },
        )
        assert r.status_code == 401
    finally:
        del os.environ["PEOPLEFORCE_WEBHOOK_SECRET"]


def _sign_body(secret: str, body: bytes) -> str:
    d = hmac.new(secret.encode("utf-8"), body, hashlib.sha256).hexdigest()
    return f"sha256={d}"


@patch("src.peopleforce.webhook_routes.connect", _fake_connect)
def test_peopleforce_webhook_valid_signature_upsert() -> None:
    secret = "signing-test-secret"
    os.environ["PEOPLEFORCE_WEBHOOK_SECRET"] = secret
    try:
        data = {
            "action": "employee_create",
            "data": _sample_employee_data(500),
        }
        raw = json.dumps(data).encode("utf-8")
        sig = _sign_body(secret, raw)
        r = _pf_client.post(
            "/peopleforce/webhook",
            content=raw,
            headers={
                "content-type": "application/json",
                "x-peopleforce-signature": sig,
            },
        )
        assert r.status_code == 200
        assert r.json()["id"] == 500
    finally:
        if os.environ.get("PEOPLEFORCE_WEBHOOK_SECRET") == secret:
            del os.environ["PEOPLEFORCE_WEBHOOK_SECRET"]


@patch("src.peopleforce.webhook_routes.connect", _fake_connect)
def test_peopleforce_webhook_leave_request_create() -> None:
    body = {
        "action": "leave_request_create",
        "data": {
            "id": 9001,
            "employee_id": 1,
            "leave_type": "Vacation",
            "state": "pending",
            "amount": 1.0,
            "tracking_time_in": "days",
            "starts_on": "2026-01-10",
            "ends_on": "2026-01-12",
            "created_at": "2026-01-01T00:00:00Z",
            "updated_at": "2026-01-01T00:00:00Z",
        },
    }
    r = _pf_client.post("/peopleforce/webhook", json=body)
    assert r.status_code == 200
    j = r.json()
    assert j.get("ok") is True
    assert j.get("entity") == "leave_request"
    assert j.get("id") == 9001


@patch("src.peopleforce.webhook_routes.connect", _fake_connect)
def test_peopleforce_webhook_vacancy_create() -> None:
    body = {
        "action": "vacancy_create",
        "data": {
            "id": 200001,
            "title": "CMO",
            "state": "open",
        },
    }
    r = _pf_client.post("/peopleforce/webhook", json=body)
    assert r.status_code == 200
    j = r.json()
    assert j.get("ok") is True
    assert j.get("entity") == "recruitment_vacancy"
    assert j.get("id") == 200001


@patch("src.peopleforce.webhook_routes.connect", _fake_connect)
def test_peopleforce_webhook_unsupported_action_skipped() -> None:
    body = {"action": "document_uploaded", "data": {"id": 1}}
    r = _pf_client.post("/peopleforce/webhook", json=body)
    assert r.status_code == 200
    j = r.json()
    assert j.get("ok") is True
    assert "skipped" in j
