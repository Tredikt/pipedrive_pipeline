-- Заполнить master.hr_employee.pipedrive_user_id и pipedrive_person_id по совпадению email
-- с витриной pipedrive_dm (нижний регистр, trim).
--
-- Если у нескольких записей в одном источнике один и тот же email, берётся строка с меньшим id.
-- Запуск после синка Pipedrive и загрузки HR master.

-- Пользователи CRM (users)
WITH u AS (
    SELECT DISTINCT ON (lower(trim(email)))
        id,
        lower(trim(email)) AS em
    FROM pipedrive_dm.pipedrive_user
    WHERE email IS NOT NULL
      AND trim(email) <> ''
    ORDER BY lower(trim(email)), id
)
UPDATE master.hr_employee h
SET pipedrive_user_id = u.id
FROM u
WHERE h.email IS NOT NULL
  AND trim(h.email) <> ''
  AND lower(trim(h.email)) = u.em;

-- Контакты (persons)
WITH p AS (
    SELECT DISTINCT ON (lower(trim(primary_email)))
        id,
        lower(trim(primary_email)) AS em
    FROM pipedrive_dm.person
    WHERE primary_email IS NOT NULL
      AND trim(primary_email) <> ''
    ORDER BY lower(trim(primary_email)), id
)
UPDATE master.hr_employee h
SET pipedrive_person_id = p.id
FROM p
WHERE h.email IS NOT NULL
  AND trim(h.email) <> ''
  AND lower(trim(h.email)) = p.em;
