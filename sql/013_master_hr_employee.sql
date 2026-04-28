-- Единый справочник сотрудников HR (источник: экспорт HR / CSV).
-- Связки: PeopleForce (pf_*), Jira (jira_*), далее — pipedrive_* по email/id через UPDATE или отдельный скрипт.

CREATE SCHEMA IF NOT EXISTS master;

CREATE TABLE IF NOT EXISTS master.hr_employee (
    hr_master_id            BIGINT PRIMARY KEY,
    pf_id                   BIGINT,
    pf_full_name            TEXT,
    jira_id                 TEXT,
    jira_full_name          TEXT,
    -- Заполняются при сопоставлении с витринами CRM (без жёстких FK — порядок миграций может различаться)
    pipedrive_user_id       BIGINT,
    pipedrive_person_id     BIGINT,
    employment_company      TEXT,
    employment_format       TEXT,
    has_leaves              BOOLEAN,
    status                  TEXT,
    gender                  TEXT,
    email                   TEXT,
    mobile_number           TEXT,
    date_of_birth           DATE,
    probation_ends_on       DATE,
    hired_on                DATE,
    position                TEXT,
    job_level               TEXT,
    location                TEXT,
    employment_type         TEXT,
    department              TEXT,
    division                TEXT,
    reporting_to            TEXT,
    reporting_to_email      TEXT,
    exception_note          TEXT,
    source_created_at       TIMESTAMPTZ,
    source_updated_at       TIMESTAMPTZ,
    loaded_at               TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_master_hr_employee_pf_id
    ON master.hr_employee (pf_id)
    WHERE pf_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS ix_master_hr_employee_email_lower
    ON master.hr_employee (LOWER(TRIM(email)))
    WHERE email IS NOT NULL AND TRIM(email) <> '';

CREATE INDEX IF NOT EXISTS ix_master_hr_employee_jira_id
    ON master.hr_employee (jira_id)
    WHERE jira_id IS NOT NULL AND TRIM(jira_id) <> '';

CREATE INDEX IF NOT EXISTS ix_master_hr_employee_pipedrive_user
    ON master.hr_employee (pipedrive_user_id)
    WHERE pipedrive_user_id IS NOT NULL;

COMMENT ON TABLE master.hr_employee IS 'Мастер-список сотрудников для связки PeopleForce / Jira / Pipedrive (поля pipedrive_* задаются отдельно).';
