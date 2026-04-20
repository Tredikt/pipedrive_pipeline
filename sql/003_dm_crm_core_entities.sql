-- Ключевые CRM-таблицы: структура по типичным полям GET /v1/* (Pipedrive API v1)
-- Кастомные поля — в pipedrive_dm.custom_field_value + pipedrive_raw.field_definition

CREATE TABLE IF NOT EXISTS pipedrive_dm.activity (
    id BIGINT PRIMARY KEY,
    subject                 TEXT,
    type                    TEXT,
    deal_id                 BIGINT,
    person_id               BIGINT,
    org_id                  BIGINT,
    owner_user_id           BIGINT REFERENCES pipedrive_dm.pipedrive_user (id),
    user_id                 BIGINT,
    group_id                BIGINT,
    company_id              BIGINT,
    due_date                DATE,
    due_time                TEXT,
    duration                TEXT,
    note                    TEXT,
    busy_flag               BOOLEAN,
    public_description      TEXT,
    done                    BOOLEAN,
    location                TEXT,
    add_time                TIMESTAMPTZ,
    update_time             TIMESTAMPTZ,
    marked_as_done_time     TIMESTAMPTZ,
    synced_at               TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_activity_deal ON pipedrive_dm.activity (deal_id);
CREATE INDEX IF NOT EXISTS idx_activity_person ON pipedrive_dm.activity (person_id);
CREATE INDEX IF NOT EXISTS idx_activity_org ON pipedrive_dm.activity (org_id);

CREATE TABLE IF NOT EXISTS pipedrive_dm.lead (
    id                      TEXT PRIMARY KEY,
    title                   TEXT,
    owner_user_id           BIGINT REFERENCES pipedrive_dm.pipedrive_user (id),
    person_id               BIGINT,
    organization_id         BIGINT,
    pipeline_id             BIGINT,
    stage_id                BIGINT,
    value                   NUMERIC,
    currency                TEXT,
    expected_close_date     DATE,
    source_name             TEXT,
    archived                BOOLEAN,
    was_seen                BOOLEAN,
    label_ids               INT[],
    add_time                TIMESTAMPTZ,
    update_time             TIMESTAMPTZ,
    synced_at               TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_lead_person ON pipedrive_dm.lead (person_id);
CREATE INDEX IF NOT EXISTS idx_lead_org ON pipedrive_dm.lead (organization_id);

CREATE TABLE IF NOT EXISTS pipedrive_dm.product (
    id                      BIGINT PRIMARY KEY,
    name                    TEXT,
    code                    TEXT,
    unit                    TEXT,
    tax                     NUMERIC,
    category                TEXT,
    owner_user_id           BIGINT REFERENCES pipedrive_dm.pipedrive_user (id),
    active_flag             BOOLEAN,
    selectable              BOOLEAN,
    first_char              TEXT,
    prices                  JSONB,
    add_time                TIMESTAMPTZ,
    update_time             TIMESTAMPTZ,
    synced_at               TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS pipedrive_dm.note (
    id BIGINT PRIMARY KEY,
    content                     TEXT,
    deal_id                     BIGINT,
    person_id                   BIGINT,
    org_id                      BIGINT,
    lead_id                     TEXT,
    project_id                  BIGINT,
    add_user_id                 BIGINT REFERENCES pipedrive_dm.pipedrive_user (id),
    update_user_id              BIGINT,
    pinned_to_deal_flag         BOOLEAN,
    pinned_to_organization_flag BOOLEAN,
    pinned_to_person_flag       BOOLEAN,
    add_time                    TIMESTAMPTZ,
    update_time                 TIMESTAMPTZ,
    synced_at                   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_note_deal ON pipedrive_dm.note (deal_id);
CREATE INDEX IF NOT EXISTS idx_note_person ON pipedrive_dm.note (person_id);

CREATE TABLE IF NOT EXISTS pipedrive_dm.call_log (
    id                  TEXT PRIMARY KEY,
    activity_id         BIGINT,
    deal_id             BIGINT,
    person_id           BIGINT,
    org_id              BIGINT,
    lead_id             TEXT,
    user_id             BIGINT REFERENCES pipedrive_dm.pipedrive_user (id),
    subject             TEXT,
    duration            NUMERIC,
    outcome             TEXT,
    from_phone_number   TEXT,
    to_phone_number     TEXT,
    start_time          TIMESTAMPTZ,
    end_time            TIMESTAMPTZ,
    note                TEXT,
    company_id          BIGINT,
    add_time            TIMESTAMPTZ,
    update_time         TIMESTAMPTZ,
    synced_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS pipedrive_dm.file (
    id                  BIGINT PRIMARY KEY,
    name                TEXT,
    file_type           TEXT,
    file_size           BIGINT,
    remote_location     TEXT,
    s3_bucket           TEXT,
    url                 TEXT,
    deal_id             BIGINT,
    person_id           BIGINT,
    org_id              BIGINT,
    product_id          BIGINT,
    lead_id             TEXT,
    activity_id         BIGINT,
    project_id          BIGINT,
    mail_message_id     BIGINT,
    log_id              TEXT,
    add_user_id         BIGINT REFERENCES pipedrive_dm.pipedrive_user (id),
    cid                 TEXT,
    add_time            TIMESTAMPTZ,
    update_time         TIMESTAMPTZ,
    synced_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS pipedrive_dm.project (
    id                  BIGINT PRIMARY KEY,
    title               TEXT,
    status              TEXT,
    owner_user_id       BIGINT REFERENCES pipedrive_dm.pipedrive_user (id),
    pipeline_id         BIGINT,
    phase_id            BIGINT,
    start_date          DATE,
    end_date            DATE,
    description TEXT,
    add_time            TIMESTAMPTZ,
    update_time         TIMESTAMPTZ,
    synced_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Расширение профиля пользователя (GET /v1/users)
ALTER TABLE pipedrive_dm.pipedrive_user ADD COLUMN IF NOT EXISTS lang TEXT;
ALTER TABLE pipedrive_dm.pipedrive_user ADD COLUMN IF NOT EXISTS locale TEXT;
ALTER TABLE pipedrive_dm.pipedrive_user ADD COLUMN IF NOT EXISTS timezone_name TEXT;
ALTER TABLE pipedrive_dm.pipedrive_user ADD COLUMN IF NOT EXISTS phone TEXT;
ALTER TABLE pipedrive_dm.pipedrive_user ADD COLUMN IF NOT EXISTS activated BOOLEAN;
ALTER TABLE pipedrive_dm.pipedrive_user ADD COLUMN IF NOT EXISTS last_login TIMESTAMPTZ;
ALTER TABLE pipedrive_dm.pipedrive_user ADD COLUMN IF NOT EXISTS created TIMESTAMPTZ;
ALTER TABLE pipedrive_dm.pipedrive_user ADD COLUMN IF NOT EXISTS modified TIMESTAMPTZ;
ALTER TABLE pipedrive_dm.pipedrive_user ADD COLUMN IF NOT EXISTS role_id INT;
ALTER TABLE pipedrive_dm.pipedrive_user ADD COLUMN IF NOT EXISTS default_currency TEXT;
ALTER TABLE pipedrive_dm.pipedrive_user ADD COLUMN IF NOT EXISTS icon_url TEXT;
ALTER TABLE pipedrive_dm.pipedrive_user ADD COLUMN IF NOT EXISTS is_admin BOOLEAN;
ALTER TABLE pipedrive_dm.pipedrive_user ADD COLUMN IF NOT EXISTS timezone_offset TEXT;
