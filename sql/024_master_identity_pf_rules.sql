-- Правила JIRA: PF — источник правды для строк person_identity; CRM только дописывает id.
-- Очередь «не найден по email» для уведомлений менеджеру.

ALTER TABLE master.person_identity
    ADD COLUMN IF NOT EXISTS first_name TEXT,
    ADD COLUMN IF NOT EXISTS last_name TEXT;

COMMENT ON COLUMN master.person_identity.first_name IS 'Имя (PeopleForce)';
COMMENT ON COLUMN master.person_identity.last_name IS 'Фамилия (PeopleForce)';

CREATE TABLE IF NOT EXISTS master.identity_link_pending (
    id              BIGSERIAL PRIMARY KEY,
    source_system   TEXT NOT NULL,
    entity_kind     TEXT NOT NULL,
    entity_id       BIGINT NOT NULL,
    email           TEXT NOT NULL,
    detail          TEXT,
    payload         JSONB,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (source_system, entity_kind, entity_id)
);

CREATE INDEX IF NOT EXISTS ix_identity_link_pending_email
    ON master.identity_link_pending (lower(trim(email)));
CREATE INDEX IF NOT EXISTS ix_identity_link_pending_created
    ON master.identity_link_pending (created_at DESC);

COMMENT ON TABLE master.identity_link_pending IS
    'Событие CRM: email не найден в person_identity (запись создаётся только из PeopleForce)';
