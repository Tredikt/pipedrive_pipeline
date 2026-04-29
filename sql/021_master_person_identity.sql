-- Единая «склейка» личности: имя, почта, внешние id (Pipedrive / PeopleForce / Jira / GA).
-- Слияние по нормализованному email; значения из webhook дополняют пустые поля (см. identity_registry.py).

CREATE SCHEMA IF NOT EXISTS master;

CREATE TABLE IF NOT EXISTS master.person_identity (
    id                      BIGSERIAL PRIMARY KEY,
    email                   TEXT NOT NULL,
    email_norm              TEXT GENERATED ALWAYS AS (lower(trim(email))) STORED,
    full_name               TEXT,
    pipedrive_person_id     BIGINT,
    pipedrive_user_id       BIGINT,
    peopleforce_employee_id BIGINT,
    jira_id                 TEXT,
    google_analytics_id     TEXT,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_person_identity_email_norm UNIQUE (email_norm)
);

CREATE INDEX IF NOT EXISTS ix_person_identity_pipedrive_person
    ON master.person_identity (pipedrive_person_id)
    WHERE pipedrive_person_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS ix_person_identity_pipedrive_user
    ON master.person_identity (pipedrive_user_id)
    WHERE pipedrive_user_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS ix_person_identity_pf_employee
    ON master.person_identity (peopleforce_employee_id)
    WHERE peopleforce_employee_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS ix_person_identity_jira
    ON master.person_identity (jira_id)
    WHERE jira_id IS NOT NULL AND trim(jira_id) <> '';

COMMENT ON TABLE master.person_identity IS 'Сквозной каталог пользователя по email: связки PF / Pipedrive / Jira / GA без полной копии HR CSV';

DROP TRIGGER IF EXISTS trg_person_identity_updated ON master.person_identity;
CREATE OR REPLACE FUNCTION master._touch_person_identity_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at := NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_person_identity_updated
    BEFORE UPDATE ON master.person_identity
    FOR EACH ROW EXECUTE PROCEDURE master._touch_person_identity_updated_at();
