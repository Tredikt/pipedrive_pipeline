# Подключение BlazeSQL к PostgreSQL (Pipedrive)

Официальная инструкция BlazeSQL: [Connecting Your Database](https://help.blazesql.com/en/articles/13586274-connecting-your-database). Поддерживается **PostgreSQL**.

## 1. Доступ к базе с интернета

Если PostgreSQL на сервере (например `217.25.88.237:5436`):

- Откройте порт PostgreSQL в **фаерволе** / security group.
- Для **веб-приложения** BlazeSQL добавьте в whitelist IP сервиса: **`35.192.145.209`** (указано в [документации BlazeSQL](https://help.blazesql.com/en/articles/13586274-connecting-your-database)).
- Альтернатива: [десктоп-клиент BlazeSQL](https://www.blazesql.com/download) в вашей сети/VPN — тогда whitelist обычно по IP вашей машины, без IP BlazeSQL.

## 2. Параметры подключения в BlazeSQL

В форме **Add new → PostgreSQL → Connect directly** укажите:

| Поле | Значение |
|------|----------|
| Host | IP или DNS сервера (без `https://`) |
| Port | например `5436` или `5432` |
| Database name | имя БД из `DATABASE_URL` (у вас в compose часто `pipedrive`) |
| Username | пользователь PostgreSQL |
| Password | пароль пользователя |

Строка из `.env` вида `postgresql://USER:PASS@HOST:PORT/DBNAME` даёт все поля, кроме того что пароль может содержать символы — их при необходимости [URL-декодируйте](https://developer.mozilla.org/en-US/docs/Glossary/Percent-encoding).

## 3. Схемы и таблицы

Данные витрины лежат **не в `public`**, а в:

- **`pipedrive_dm`** — основные таблицы (`person`, `deal`, `organization`, `custom_field_value`, …) и представление **`v_custom_fields_labeled`**
- **`pipedrive_raw`** — каталог полей `field_definition` и при необходимости `entity_record`

На шаге **Select tables** в BlazeSQL отметьте нужные объекты из этих схем (не обязательно все300 таблиц — только то, что нужно для вопросов).

Если в интерфейсе таблицы из нестандартных схем не видны, создайте в PostgreSQL роль только для чтения с `search_path`:

```sql
CREATE ROLE blazesql_ro WITH LOGIN PASSWORD 'сгенерируйте_надёжный';
GRANT CONNECT ON DATABASE pipedrive TO blazesql_ro;
GRANT USAGE ON SCHEMA pipedrive_dm, pipedrive_raw TO blazesql_ro;
GRANT SELECT ON ALL TABLES IN SCHEMA pipedrive_dm, pipedrive_raw TO blazesql_ro;
GRANT SELECT ON ALL SEQUENCES IN SCHEMA pipedrive_dm, pipedrive_raw TO blazesql_ro;
ALTER ROLE blazesql_ro IN DATABASE pipedrive SET search_path TO pipedrive_dm, pipedrive_raw, public;
```

Подставьте имя базы вместо `pipedrive`, если у вас другое.

В BlazeSQL подключайтесь пользователем **`blazesql_ro`**.

Готовый скрипт-шаблон: `sql/005_blazesql_readonly_role.sql`.

## 4. Подсказки для NL-запросов

- Готовые примеры SQL (ТЗ: дашборды / образцы): **`sql/bi_example_queries.sql`** — можно импортировать в BlazeSQL или использовать как основу карточек.
- Кастомные поля: таблица **`pipedrive_dm.custom_field_value`** или представление **`pipedrive_dm.v_custom_fields_labeled`** — колонка **`field_name`** (человекочитаемое имя из Pipedrive).
- Связи: `person.org_id` → `organization.id`, `deal.person_id` → `person.id` и т.д.

## 5. Ограничения и приватность

- BlazeSQL для генерации SQL использует **метаданные** (имена таблиц и колонок); детали — в [Data Privacy & Security](https://help.blazesql.com/en/collections/18231547-data-privacy-security).
- Для максимальной приватности результатов запросов используйте **desktop**-версию в закрытой сети.
