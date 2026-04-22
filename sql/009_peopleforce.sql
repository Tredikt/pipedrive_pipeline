-- Витрина и сырой слой PeopleForce (API v3 public, см. developer.peopleforce.io)
CREATE SCHEMA IF NOT EXISTS peopleforce_raw;

CREATE TABLE IF NOT EXISTS peopleforce_raw.entity_record (
    entity_type   TEXT    NOT NULL,
    external_id   TEXT    NOT NULL,
    raw           JSONB   NOT NULL,
    synced_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (entity_type, external_id)
);

CREATE INDEX IF NOT EXISTS idx_peopleforce_entity_type ON peopleforce_raw.entity_record (entity_type);
CREATE INDEX IF NOT EXISTS idx_peopleforce_raw_gin ON peopleforce_raw.entity_record USING GIN (raw jsonb_path_ops);

COMMENT ON SCHEMA peopleforce_raw IS 'JSON ответов PeopleForce API v3 (сырой слой)';

CREATE SCHEMA IF NOT EXISTS peopleforce_dm;

-- Справочники (list endpoints, плоские объекты)
CREATE TABLE IF NOT EXISTS peopleforce_dm.department (
    id                  BIGINT PRIMARY KEY,
    name                TEXT NOT NULL,
    parent_id           BIGINT,
    manager_id          BIGINT,
    department_level_id BIGINT,
    synced_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS peopleforce_dm.division (
    id        BIGINT PRIMARY KEY,
    name      TEXT,
    synced_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS peopleforce_dm.location (
    id        BIGINT PRIMARY KEY,
    name      TEXT,
    synced_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS peopleforce_dm.position (
    id        BIGINT PRIMARY KEY,
    name      TEXT,
    synced_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS peopleforce_dm.job_level (
    id         BIGINT PRIMARY KEY,
    name       TEXT,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ,
    synced_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS peopleforce_dm.employment_type (
    id         BIGINT PRIMARY KEY,
    name       TEXT,
    created_at TIMESTAMPTZ,
    synced_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Сотрудник: плоские поля + внешние ключи к справочникам (id из вложенных объектов API)
CREATE TABLE IF NOT EXISTS peopleforce_dm.employee (
    id                  BIGINT PRIMARY KEY,
    active              BOOLEAN,
    access              BOOLEAN,
    employee_number     TEXT,
    full_name           TEXT,
    first_name          TEXT,
    middle_name         TEXT,
    last_name           TEXT,
    email               TEXT,
    personal_email      TEXT,
    mobile_number       TEXT,
    work_phone_number   TEXT,
    date_of_birth       DATE,
    gender              TEXT,
    avatar_url          TEXT,
    probation_ends_on   DATE,
    hired_on            DATE,
    skype_username      TEXT,
    slack_username      TEXT,
    twitter_username    TEXT,
    facebook_url        TEXT,
    linkedin_url        TEXT,
    position_id         BIGINT REFERENCES peopleforce_dm.position (id),
    job_level_id        BIGINT REFERENCES peopleforce_dm.job_level (id),
    location_id         BIGINT REFERENCES peopleforce_dm.location (id),
    employment_type_id  BIGINT REFERENCES peopleforce_dm.employment_type (id),
    division_id         BIGINT REFERENCES peopleforce_dm.division (id),
    department_id       BIGINT REFERENCES peopleforce_dm.department (id),
    manager_employee_id BIGINT,
    reporting_to_id     BIGINT,
    synced_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pf_emp_email ON peopleforce_dm.employee (email);
CREATE INDEX IF NOT EXISTS idx_pf_emp_department ON peopleforce_dm.employee (department_id);
CREATE INDEX IF NOT EXISTS idx_pf_emp_manager ON peopleforce_dm.employee (manager_employee_id);

COMMENT ON SCHEMA peopleforce_dm IS 'Нормализованная витрина PeopleForce для BI/отчётов';
