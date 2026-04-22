from __future__ import annotations

from src.peopleforce.bulk_endpoints import default_entity_type
from src.peopleforce.bulk_raw import external_id_for_row
from src.peopleforce.dm_extra import _to_int_id, _to_str_list


def test_default_entity_type_path():
    assert default_entity_type("/recruitment/vacancies") == "recruitment_vacancies"
    assert default_entity_type("/leave_requests") == "leave_requests"


def test_external_id_prefers_id():
    assert external_id_for_row({"id": 7, "x": 1}) == "7"
    assert external_id_for_row({"uuid": "abc"}) == "abc"


def test_to_str_list_skills():
    assert _to_str_list(["IFRS", "SAP"]) == ["IFRS", "SAP"]
    assert _to_str_list([]) is None


def test_to_int_id_gender():
    assert _to_int_id(3535) == 3535
    assert _to_int_id(None) is None


def test_external_id_no_id():
    a = external_id_for_row({"a": 1})
    b = external_id_for_row({"a": 1})
    c = external_id_for_row({"a": 2})
    assert a == b
    assert a != c
    assert a.startswith("sha256:")
