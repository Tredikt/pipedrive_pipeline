-- В представлении: оригинальное имя поля из Pipedrive (как в админке / *Fields name)
CREATE OR REPLACE VIEW pipedrive_dm.v_custom_fields_labeled AS
SELECT
    cf.entity_type,
    cf.entity_id,
    cf.field_key,
    cf.field_name,
    fd.field_name      AS pipedrive_field_name,
    fd.field_type,
    fd.options         AS field_options,
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
'field_name — sanitized имя в EAV; pipedrive_field_name — name из Pipedrive API (*Fields), для сверки со списком полей в CRM';
