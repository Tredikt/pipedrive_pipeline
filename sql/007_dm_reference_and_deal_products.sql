-- Справочники для BI (раньше только pipedrive_raw.entity_record) + продукты по сделкам
CREATE TABLE IF NOT EXISTS pipedrive_dm.pipeline (
    id                      BIGINT PRIMARY KEY,
    name                    TEXT,
    url_title               TEXT,
    order_nr                INT,
    active                  BOOLEAN,
    deal_probability        BOOLEAN,
    selected                BOOLEAN,
    add_time                TIMESTAMPTZ,
    update_time             TIMESTAMPTZ,
    synced_at               TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS pipedrive_dm.stage (
    id                      BIGINT PRIMARY KEY,
    pipeline_id             BIGINT REFERENCES pipedrive_dm.pipeline (id),
    name                    TEXT,
    order_nr                INT,
    deal_probability        NUMERIC,
    rotten_days             INT,
    rotten_flag             BOOLEAN,
    active_flag             BOOLEAN,
    add_time                TIMESTAMPTZ,
    update_time             TIMESTAMPTZ,
    synced_at               TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_stage_pipeline ON pipedrive_dm.stage (pipeline_id);

CREATE TABLE IF NOT EXISTS pipedrive_dm.currency (
    id                      BIGINT PRIMARY KEY,
    code                    TEXT,
    name                    TEXT,
    decimal_points          INT,
    symbol                  TEXT,
    active_flag             BOOLEAN,
    is_default              BOOLEAN,
    is_custom               BOOLEAN,
    symbol_before_amount    BOOLEAN,
    synced_at               TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- GET /v1/dealProducts — позиции товаров в сделке
CREATE TABLE IF NOT EXISTS pipedrive_dm.deal_product_line (
    id                      BIGINT PRIMARY KEY,
    deal_id                 BIGINT NOT NULL,
    product_id              BIGINT,
    name                    TEXT,
    quantity                NUMERIC,
    item_price              NUMERIC,
    sum                     NUMERIC,
    currency                TEXT,
    product_variation_id    BIGINT,
    discount_percentage     NUMERIC,
    duration                INT,
    duration_unit           TEXT,
    tax                     NUMERIC,
    tax_method              TEXT,
    enabled_flag            BOOLEAN,
    add_time                TIMESTAMPTZ,
    update_time             TIMESTAMPTZ,
    synced_at               TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_deal_product_line_deal ON pipedrive_dm.deal_product_line (deal_id);
CREATE INDEX IF NOT EXISTS idx_deal_product_line_product ON pipedrive_dm.deal_product_line (product_id);
