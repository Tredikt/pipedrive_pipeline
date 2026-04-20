-- Примеры запросов для BlazeSQL / ThoughtSpot (ТЗ §2.2: 3–5 дашбордов / примеров).
-- Схема витрины: pipedrive_dm; полный JSON API: pipedrive_raw.entity_record.

-- 1) Сделки по воронке и этапу (основной CRM-срез)
SELECT d.id,
       d.title,
       d.value,
       d.currency,
       p.name  AS pipeline_name,
       s.name  AS stage_name,
       d.status,
       d.add_time
FROM pipedrive_dm.deal d
LEFT JOIN pipedrive_dm.pipeline p ON p.id = d.pipeline_id
LEFT JOIN pipedrive_dm.stage s ON s.id = d.stage_id
ORDER BY d.update_time DESC NULLS LAST
LIMIT 500;

-- 2) Контакты с человекочитаемыми кастомными полями (маппинг *Fields)
SELECT p.id,
       p.name,
       p.primary_email,
       cf.field_name,
       COALESCE(cf.value_text, cf.value_json::text) AS value
FROM pipedrive_dm.person p
JOIN pipedrive_dm.custom_field_value cf
  ON cf.entity_type = 'person' AND cf.entity_id = p.id::text
ORDER BY p.id, cf.field_name
LIMIT 1000;

-- 3) Активности с типом «звонок» (когда call_log из API пуст)
SELECT *
FROM pipedrive_dm.v_phone_calls_from_activities
ORDER BY due_date DESC NULLS LAST
LIMIT 500;

-- 4) Товарные позиции по сделкам (DealProducts)
SELECT l.deal_id,
       d.title       AS deal_title,
       l.name        AS line_name,
       l.quantity,
       l.item_price,
       l.sum,
       l.currency
FROM pipedrive_dm.deal_product_line l
JOIN pipedrive_dm.deal d ON d.id = l.deal_id
ORDER BY l.deal_id, l.id
LIMIT 1000;

-- 5) Объём выгрузки по типам сущностей (сырой слой — контроль полноты)
SELECT entity_type,
       COUNT(*) AS rows
FROM pipedrive_raw.entity_record
GROUP BY entity_type
ORDER BY rows DESC;
