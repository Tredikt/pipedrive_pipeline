from __future__ import annotations

from src.peopleforce.parse import _to_text, flat_employee_row, merge_webhook_employee_data


def test_flat_employee_list_shape():
    raw = {
        "id": 10,
        "full_name": "A B",
        "active": True,
        "position": {"id": 1, "name": "Dev"},
        "department": {"id": 2, "name": "IT"},
    }
    f = flat_employee_row(raw)
    assert f["id"] == 10
    assert f["position_id"] == 1
    assert f["department_id"] == 2


def test_to_text_nested_enum():
    assert _to_text({"value": "male"}) == "male"
    assert _to_text({"name": "X"}) == "X"


def test_flat_employee_gender_object():
    raw = {
        "id": 1,
        "full_name": "A",
        "active": True,
        "gender": {"value": "female"},
    }
    f = flat_employee_row(raw)
    assert f["gender"] == "female"


def test_webhook_shape_with_attributes():
    data = {
        "id": 5,
        "attributes": {"first_name": "X", "email": "a@b.c"},
        "position": {"id": 1, "name": "P"},
    }
    m = merge_webhook_employee_data(data)
    f = flat_employee_row(m)
    assert f["id"] == 5
    assert f["email"] == "a@b.c"
    assert f["position_id"] == 1
