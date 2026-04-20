-- Кастомные поля: хеш-ключ API + человекочитаемое имя из *Fields (pipedrive_raw.field_definition).
-- Для BI: смотрите field_name и value_text; field_key — внутренний id Pipedrive.

CREATE OR REPLACE VIEW pipedrive_dm.v_custom_fields_labeled AS
SELECT
    cf.entity_type,
    cf.entity_id,
    cf.field_key,
    cf.field_name,
    fd.field_type,
    fd.options          AS field_options,
    cf.value_text,
    cf.value_json,
    cf.synced_at
FROM pipedrive_dm.custom_field_value cf
LEFT JOIN pipedrive_raw.field_definition fd
    ON fd.field_key = cf.field_key
   AND fd.entity_type = (
        CASE cf.entity_type
            WHEN 'person' THEN 'persons'
            WHEN 'organization' THEN 'organizations'
            WHEN 'deal' THEN 'deals'
            WHEN 'activity' THEN 'activities'
            WHEN 'lead' THEN 'leads'
            WHEN 'product' THEN 'products'
            WHEN 'note' THEN 'notes'
            WHEN 'call_log' THEN 'call_logs'
            WHEN 'project' THEN 'projects'
            ELSE cf.entity_type
        END
    );

COMMENT ON VIEW pipedrive_dm.v_custom_fields_labeled IS
'Кастомные поля: field_name — адекватное имя из Pipedrive (*Fields), field_key — токен из JSON API';
