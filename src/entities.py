from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class EntitySpec:
    """Описание сущности Pipedrive API v1."""

    name: str
    list_path: str
    # Эндпоинт метаданных полей (если есть) — для маппинга custom key -> name
    fields_path: str | None = None
    id_key: str = "id"
    # Если задано: размер страницы (у callLogs max 50, см. доку)
    page_size: int | None = None


# Пути по документации v1; при ошибках 404 сверяйте с актуальным референсом API.
ENTITY_SPECS: tuple[EntitySpec, ...] = (
    EntitySpec("deals", "/v1/deals", "/v1/dealFields"),
    EntitySpec("persons", "/v1/persons", "/v1/personFields"),
    EntitySpec("organizations", "/v1/organizations", "/v1/organizationFields"),
    EntitySpec("activities", "/v1/activities", "/v1/activityFields"),
    EntitySpec("leads", "/v1/leads", "/v1/leadFields"),
    EntitySpec("products", "/v1/products", "/v1/productFields"),
    EntitySpec("notes", "/v1/notes", "/v1/noteFields"),
    EntitySpec("call_logs", "/v1/callLogs", None, page_size=50),
    EntitySpec("files", "/v1/files", None),
    EntitySpec("pipelines", "/v1/pipelines", None),
    EntitySpec("stages", "/v1/stages", None),
    EntitySpec("activity_types", "/v1/activityTypes", None),
    EntitySpec("lead_labels", "/v1/leadLabels", None),
    EntitySpec("lead_sources", "/v1/leadSources", None),
    EntitySpec("currencies", "/v1/currencies", None),
    EntitySpec("users", "/v1/users", None),
    EntitySpec("roles", "/v1/roles", None),
    EntitySpec("legacy_teams", "/v1/legacyTeams", None),
    EntitySpec("user_connections", "/v1/userConnections", None),
    EntitySpec("user_settings", "/v1/userSettings", None),
    EntitySpec("projects", "/v1/projects", "/v1/projectFields"),
    EntitySpec("project_templates", "/v1/projectTemplates", None),
    EntitySpec("meetings", "/v1/meetings", None),
    EntitySpec("goals", "/v1/goals", None),
    EntitySpec("filters", "/v1/filters", None),
    EntitySpec("channels", "/v1/channels", None),
    EntitySpec("deal_products", "/v1/dealProducts", None),
    EntitySpec("organization_relationships", "/v1/organizationRelationships", None),
    # Сырые таблицы определений полей (дублируют ответы *Fields — удобно для отладки)
    EntitySpec("deal_fields", "/v1/dealFields", None),
    EntitySpec("person_fields", "/v1/personFields", None),
    EntitySpec("organization_fields", "/v1/organizationFields", None),
    EntitySpec("product_fields", "/v1/productFields", None),
    EntitySpec("activity_fields", "/v1/activityFields", None),
    EntitySpec("lead_fields", "/v1/leadFields", None),
    EntitySpec("note_fields", "/v1/noteFields", None),
    EntitySpec("project_fields", "/v1/projectFields", None),
)
