-- После загрузки master.hr_employee и наличия pipedrive_dm.pipedrive_user:
-- проставить pipedrive_user_id по совпадению email (нижний регистр, trim).

UPDATE master.hr_employee h
SET pipedrive_user_id = u.id
FROM pipedrive_dm.pipedrive_user u
WHERE h.email IS NOT NULL
  AND TRIM(h.email) <> ''
  AND u.email IS NOT NULL
  AND TRIM(u.email) <> ''
  AND lower(trim(h.email)) = lower(trim(u.email));
