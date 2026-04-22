"""
Реестр GET-листов PeopleForce API v3 для сырой выгрузки в peopleforce_raw.entity_record.

Пути сверены с https://developer.peopleforce.io/reference (v3). Часть путей может
вернуть 404 (устаревшее имя) или 403 (модуль/права ключа) — тогда sync их пропускает.

entity_type для пересечения со «структурным» sync: department, division, location,
employee, position, job_level, employment_type — тот же ключ, ON CONFLICT обновит raw.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class RawListEndpoint:
    path: str
    extra_params: dict[str, Any] | None = None
    entity_type: str | None = None


def _ep(
    path: str,
    *,
    extra: dict[str, Any] | None = None,
    entity_type: str | None = None,
) -> RawListEndpoint:
    return RawListEndpoint(path=path, extra_params=extra, entity_type=entity_type)


# Широкий диапазон дат для leave_requests (см. параметры starts_on / ends_on в API).
LEAVE_REQUEST_DEFAULT_PARAMS: dict[str, Any] = {
    "starts_on": "2000-01-01",
    "ends_on": "2035-12-31",
}
_LEAVE_WIDE = LEAVE_REQUEST_DEFAULT_PARAMS


# Порядок: сначала справочники, затем сотрудники, остальные модули.
RAW_LIST_ENDPOINTS: tuple[RawListEndpoint, ...] = (
    # Справочники (совпадает с peopleforce.sync + DM-справочники из вложенных полей)
    _ep("/departments", entity_type="department"),
    _ep("/divisions", entity_type="division"),
    _ep("/locations", entity_type="location"),
    _ep("/positions", entity_type="position"),
    _ep("/job_levels", entity_type="job_level"),
    _ep("/employment_types", entity_type="employment_type"),
    _ep("/department_levels"),
    _ep("/teams"),
    # Сотрудники
    _ep("/employees", extra={"status": "all"}, entity_type="employee"),
    # Leave & календари
    _ep("/leave_requests", extra=LEAVE_REQUEST_DEFAULT_PARAMS),
    _ep("/leave_types"),
    _ep("/public_holidays"),
    _ep("/company_holidays"),
    # Время / графики
    _ep("/work_schedules"),
    _ep("/working_patterns"),
    _ep("/time_entries"),
    _ep("/shifts"),
    # Подбор
    _ep("/recruitment/vacancies"),
    _ep("/recruitment/candidates"),
    _ep("/recruitment/applications"),
    # Payroll / прочее
    _ep("/pay_schedules"),
    _ep("/custom_fields/employees"),
    _ep("/assets"),
    _ep("/document_types"),
    _ep("/cost_centers"),
    _ep("/projects"),
)


def default_entity_type(path: str) -> str:
    """Стабильное имя сущности из пути: /recruitment/vacancies -> recruitment_vacancies."""
    p = path.strip("/")
    return p.replace("/", "_").replace("-", "_")


def entity_type_for_path(path: str) -> str:
    """Имя для peopleforce_raw / совместимость: учитывает overrides из RAW_LIST_ENDPOINTS."""
    for ep in RAW_LIST_ENDPOINTS:
        if ep.path == path and ep.entity_type:
            return ep.entity_type
    return default_entity_type(path)
