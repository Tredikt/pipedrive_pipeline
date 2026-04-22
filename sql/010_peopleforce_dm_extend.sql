-- Расширение витрины PeopleForce: сущности list API v3 (помимо 009).
-- payload = полный объект ответа API (детали, вложенные массивы).

CREATE TABLE IF NOT EXISTS peopleforce_dm.department_level (
    id         BIGINT PRIMARY KEY,
    name       TEXT,
    sort_order INT,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ,
    payload    JSONB   NOT NULL,
    synced_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS peopleforce_dm.team (
    id                 BIGINT PRIMARY KEY,
    name               TEXT,
    team_lead_id       BIGINT,
    team_lead_email    TEXT,
    team_lead_full_name TEXT,
    created_at         TIMESTAMPTZ,
    updated_at         TIMESTAMPTZ,
    payload            JSONB   NOT NULL,
    synced_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS peopleforce_dm.leave_type (
    id         BIGINT PRIMARY KEY,
    name       TEXT,
    unit       TEXT,
    fa_class   TEXT,
    hex_color  TEXT,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ,
    payload    JSONB   NOT NULL,
    synced_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS peopleforce_dm.leave_request (
    id                 BIGINT PRIMARY KEY,
    employee_id        BIGINT,
    leave_type_id      BIGINT,
    leave_type_name    TEXT,
    state              TEXT,
    amount             DOUBLE PRECISION,
    tracking_time_in   TEXT,
    on_demand          BOOLEAN,
    starts_on          DATE,
    ends_on            DATE,
    comment            TEXT,
    created_at         TIMESTAMPTZ,
    updated_at         TIMESTAMPTZ,
    payload            JSONB   NOT NULL,
    synced_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pf_lr_employee ON peopleforce_dm.leave_request (employee_id);
CREATE INDEX IF NOT EXISTS idx_pf_lr_state ON peopleforce_dm.leave_request (state);
CREATE INDEX IF NOT EXISTS idx_pf_lr_dates ON peopleforce_dm.leave_request (starts_on, ends_on);

CREATE TABLE IF NOT EXISTS peopleforce_dm.public_holiday (
    id           BIGINT PRIMARY KEY,
    name         TEXT,
    event_date   DATE,
    country_code TEXT,
    created_at   TIMESTAMPTZ,
    updated_at   TIMESTAMPTZ,
    payload      JSONB   NOT NULL,
    synced_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS peopleforce_dm.company_holiday (
    id         BIGINT PRIMARY KEY,
    name       TEXT,
    event_date DATE,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ,
    payload    JSONB   NOT NULL,
    synced_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS peopleforce_dm.work_schedule (
    id         BIGINT PRIMARY KEY,
    name       TEXT,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ,
    payload    JSONB   NOT NULL,
    synced_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS peopleforce_dm.working_pattern (
    id         BIGINT PRIMARY KEY,
    name       TEXT,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ,
    payload    JSONB   NOT NULL,
    synced_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS peopleforce_dm.time_entry (
    id          BIGINT PRIMARY KEY,
    name        TEXT,
    employee_id BIGINT,
    started_at  TIMESTAMPTZ,
    ended_at    TIMESTAMPTZ,
    created_at  TIMESTAMPTZ,
    updated_at  TIMESTAMPTZ,
    payload     JSONB   NOT NULL,
    synced_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pf_time_emp ON peopleforce_dm.time_entry (employee_id);

CREATE TABLE IF NOT EXISTS peopleforce_dm.shift (
    id         BIGINT PRIMARY KEY,
    name       TEXT,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ,
    payload    JSONB   NOT NULL,
    synced_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS peopleforce_dm.recruitment_vacancy (
    id          BIGINT PRIMARY KEY,
    name        TEXT,
    state       TEXT,
    title       TEXT,
    description TEXT,
    tags        TEXT[],
    skills      TEXT[],
    created_at  TIMESTAMPTZ,
    updated_at  TIMESTAMPTZ,
    payload     JSONB   NOT NULL,
    synced_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS peopleforce_dm.recruitment_candidate (
    id              BIGINT PRIMARY KEY,
    name            TEXT,
    full_name       TEXT,
    email           TEXT,
    tags            TEXT[],
    urls            TEXT[],
    level           TEXT,
    gender          TEXT,
    gender_id       BIGINT,
    resume          BOOLEAN,
    skills          TEXT[],
    source          TEXT,
    location        TEXT,
    position        TEXT,
    cover_letter    TEXT,
    date_of_birth   DATE,
    phone_numbers   TEXT[],
    created_at      TIMESTAMPTZ,
    updated_at      TIMESTAMPTZ,
    created_by      JSONB,
    applications    JSONB,
    payload         JSONB   NOT NULL,
    synced_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS peopleforce_dm.recruitment_application (
    id                    BIGINT PRIMARY KEY,
    name                  TEXT,
    vacancy_id            BIGINT,
    candidate_id          BIGINT,
    state                 TEXT,
    created_at            TIMESTAMPTZ,
    updated_at            TIMESTAMPTZ,
    pipeline_state_id     BIGINT,
    pipeline_state_name   TEXT,
    disqualified_at       TIMESTAMPTZ,
    disqualify_reason_name TEXT,
    payload               JSONB   NOT NULL,
    synced_at             TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS peopleforce_dm.pay_schedule (
    id         BIGINT PRIMARY KEY,
    name       TEXT,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ,
    payload    JSONB   NOT NULL,
    synced_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Определения кастомных полей сотрудников (GET /custom_fields/employees)
CREATE TABLE IF NOT EXISTS peopleforce_dm.employee_custom_field (
    id            BIGINT PRIMARY KEY,
    name          TEXT,
    field_key     TEXT,
    data_type     TEXT,
    field_position INT,
    required      BOOLEAN,
    created_at    TIMESTAMPTZ,
    updated_at    TIMESTAMPTZ,
    payload       JSONB   NOT NULL,
    synced_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS peopleforce_dm.asset (
    id            BIGINT PRIMARY KEY,
    name          TEXT,
    serial_number TEXT,
    asset_type    TEXT,
    status        TEXT,
    created_at    TIMESTAMPTZ,
    updated_at    TIMESTAMPTZ,
    payload       JSONB   NOT NULL,
    synced_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS peopleforce_dm.document_type (
    id         BIGINT PRIMARY KEY,
    name       TEXT,
    category   TEXT,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ,
    payload    JSONB   NOT NULL,
    synced_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS peopleforce_dm.cost_center (
    id         BIGINT PRIMARY KEY,
    name       TEXT,
    code       TEXT,
    parent_id  BIGINT,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ,
    payload    JSONB   NOT NULL,
    synced_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS peopleforce_dm.project (
    id         BIGINT PRIMARY KEY,
    name       TEXT,
    code       TEXT,
    status     TEXT,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ,
    payload    JSONB   NOT NULL,
    synced_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
