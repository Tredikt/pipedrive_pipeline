-- Сырое хранилище выгрузки Pipedrive (см. ТЗ: схема pipedrive_raw)
CREATE SCHEMA IF NOT EXISTS pipedrive_raw;

-- Метаданные полей (*Fields): key -> name для маппинга кастомных полей
CREATE TABLE IF NOT EXISTS pipedrive_raw.field_definition (
    entity_type   TEXT NOT NULL,
    field_key     TEXT NOT NULL,
    field_name    TEXT NOT NULL,
    field_type    TEXT,
    options       JSONB,
    raw           JSONB NOT NULL,
    synced_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (entity_type, field_key)
);

-- Универсальная таблица сущностей: легко добавлять новые типы без DDL
-- pipedrive_id TEXT: у leads и др. идентификатор — UUID строкой
CREATE TABLE IF NOT EXISTS pipedrive_raw.entity_record (
    entity_type       TEXT NOT NULL,
    pipedrive_id      TEXT NOT NULL,
    raw               JSONB NOT NULL,
    custom_resolved   JSONB NOT NULL DEFAULT '{}'::jsonb,
    pipedrive_updated TIMESTAMPTZ,
    synced_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (entity_type, pipedrive_id)
);

CREATE INDEX IF NOT EXISTS idx_entity_record_type_updated
    ON pipedrive_raw.entity_record (entity_type, pipedrive_updated DESC);

CREATE INDEX IF NOT EXISTS idx_entity_record_raw_gin
    ON pipedrive_raw.entity_record USING GIN (raw jsonb_path_ops);

COMMENT ON SCHEMA pipedrive_raw IS 'Сырой слой выгрузки Pipedrive + развёрнутые кастомные поля по человекочитаемым именам';

-- Старые установки: колонка была BIGINT
DO $migrate$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'pipedrive_raw'
          AND table_name = 'entity_record'
          AND column_name = 'pipedrive_id'
          AND data_type = 'bigint'
    ) THEN
        ALTER TABLE pipedrive_raw.entity_record
            ALTER COLUMN pipedrive_id TYPE TEXT USING pipedrive_id::text;
    END IF;
END $migrate$;
