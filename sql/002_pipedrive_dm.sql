-- Витрина под BI: отдельные таблицы, FK, кастомные поля в custom_field_value
CREATE SCHEMA IF NOT EXISTS pipedrive_dm;

CREATE TABLE IF NOT EXISTS pipedrive_dm.pipedrive_user (
    id              BIGINT PRIMARY KEY,
    name            TEXT,
    email           TEXT,
    active_flag     BOOLEAN,
    has_pic         INT,
    pic_hash        TEXT,
    synced_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS pipedrive_dm.organization (
    id              BIGINT PRIMARY KEY,
    name            TEXT,
    owner_user_id   BIGINT REFERENCES pipedrive_dm.pipedrive_user (id),
    people_count    INT,
    cc_email        TEXT,
    address         TEXT,
    active_flag     BOOLEAN,
    add_time        TIMESTAMPTZ,
    update_time     TIMESTAMPTZ,
    synced_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS pipedrive_dm.person (
    id                              BIGINT PRIMARY KEY,
    name                            TEXT,
    first_name                      TEXT,
    last_name                       TEXT,
    primary_email                   TEXT,
    primary_phone                   TEXT,
    job_title                       TEXT,
    org_id                          BIGINT REFERENCES pipedrive_dm.organization (id),
    owner_user_id                   BIGINT REFERENCES pipedrive_dm.pipedrive_user (id),
    label                           INT,
    label_ids                       INT[],
    visible_to                      TEXT,
    active_flag                     BOOLEAN,
    won_deals_count                 INT,
    lost_deals_count                INT,
    open_deals_count                INT,
    closed_deals_count              INT,
    related_won_deals_count         INT,
    related_lost_deals_count        INT,
    related_open_deals_count        INT,
    related_closed_deals_count      INT,
    participant_open_deals_count    INT,
    participant_closed_deals_count  INT,
    done_activities_count           INT,
    undone_activities_count         INT,
    activities_count                INT,
    email_messages_count            INT,
    files_count                     INT,
    notes_count                     INT,
    followers_count                 INT,
    last_outgoing_mail_time         TIMESTAMPTZ,
    last_incoming_mail_time         TIMESTAMPTZ,
    last_activity_id                BIGINT,
    next_activity_id                BIGINT,
    last_activity_date              DATE,
    next_activity_date              DATE,
    company_id                      BIGINT,
    add_time                        TIMESTAMPTZ,
    update_time                     TIMESTAMPTZ,
    synced_at                       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_person_org ON pipedrive_dm.person (org_id);
CREATE INDEX IF NOT EXISTS idx_person_owner ON pipedrive_dm.person (owner_user_id);
CREATE INDEX IF NOT EXISTS idx_person_email ON pipedrive_dm.person (primary_email);

CREATE TABLE IF NOT EXISTS pipedrive_dm.deal (
    id                  BIGINT PRIMARY KEY,
    title               TEXT,
    value               NUMERIC,
    currency            TEXT,
    stage_id            BIGINT,
    pipeline_id         BIGINT,
    person_id           BIGINT,
    org_id              BIGINT REFERENCES pipedrive_dm.organization (id),
    status              TEXT,
    probability         NUMERIC,
    owner_user_id       BIGINT REFERENCES pipedrive_dm.pipedrive_user (id),
    visible_to          TEXT,
    lost_reason         TEXT,
    expected_close_date DATE,
    add_time            TIMESTAMPTZ,
    update_time         TIMESTAMPTZ,
    synced_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_deal_org ON pipedrive_dm.deal (org_id);
CREATE INDEX IF NOT EXISTS idx_deal_person ON pipedrive_dm.deal (person_id);
CREATE INDEX IF NOT EXISTS idx_deal_stage ON pipedrive_dm.deal (stage_id);

-- Кастомные поля: одна строка = одно значение (имя поля из dealFields / personFields и т.д.)
CREATE TABLE IF NOT EXISTS pipedrive_dm.custom_field_value (
    entity_type   TEXT NOT NULL,
    entity_id     TEXT NOT NULL,
    field_key     TEXT NOT NULL,
    field_name    TEXT NOT NULL,
    value_text    TEXT,
    value_json    JSONB,
    synced_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (entity_type, entity_id, field_key)
);

CREATE INDEX IF NOT EXISTS idx_custom_field_entity    ON pipedrive_dm.custom_field_value (entity_type, entity_id);

CREATE INDEX IF NOT EXISTS idx_custom_field_name
    ON pipedrive_dm.custom_field_value (entity_type, field_name);

COMMENT ON SCHEMA pipedrive_dm IS 'Нормализованные сущности Pipedrive + EAV для кастомных полей';
