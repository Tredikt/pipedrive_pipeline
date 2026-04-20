-- Звонки как активности: в Pipedrive тип активности часто = 'call', тогда как отдельные Call logs (GET /v1/callLogs)
-- бывают пустыми, если нет интеграции телефонии / логов именно в этом API.
CREATE OR REPLACE VIEW pipedrive_dm.v_phone_calls_from_activities AS
SELECT
    id,
    subject,
    type,
    deal_id,
    person_id,
    org_id,
    owner_user_id,
    user_id,
    due_date,
    due_time,
    duration,
    note,
    done,
    add_time,
    update_time
FROM pipedrive_dm.activity
WHERE lower(trim(coalesce(type, ''))) = 'call';

COMMENT ON VIEW pipedrive_dm.v_phone_calls_from_activities IS
'Активности с type=call; используйте, если pipedrive_dm.call_log пуст, а звонки ведутся как активности';
