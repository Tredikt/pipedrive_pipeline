# Pipedrive → PostgreSQL (сырой слой)

Проект закрывает часть ТЗ: выгрузка сущностей [Pipedrive API v1](https://developers.pipedrive.com/docs/api/v1) в схему `pipedrive_raw`, маппинг кастомных полей (ключ API → человекочитаемое имя из `name` в `*Fields`).

## Безопасность

- Секреты только в `.env` (шаблон — `.env.example`). Не коммитьте токены.
- Если токен когда‑либо светился в документе, чате или репозитории — **отзовите и выпустите новый** в настройках Pipedrive.

## Установка

```bash
cd c:\PycharmProjects\data_pipeline
python -m venv .venv
.\.venv\Scripts\activate
pip install -r requirements.txt
copy .env.example .env
```

Заполните `.env`: `PIPEDRIVE_API_TOKEN`, `PIPEDRIVE_COMPANY_DOMAIN` (поддомен из `https://ВАШ.pipedrive.com`), `DATABASE_URL`. Если домен компании не задан — используется `PIPEDRIVE_API_BASE_URL` (по умолчанию `https://api.pipedrive.com`).

PostgreSQL в Docker — отдельная папка **`postgres-docker`** (её можно переносить целиком): см. `postgres-docker/README.md`.

## PeopleForce

Интеграция [PeopleForce API v3](https://developer.peopleforce.io/docs/getting-started): витрина **`peopleforce_dm`** (нормализованные таблицы + `payload` JSONB с полным объектом API; список сущностей — `sql/010_peopleforce_dm_extend.sql`) и при необходимости **`peopleforce_raw.entity_record`** (флаги `--raw` / отдельная команда). Аутентификация: **`X-API-KEY`** ([док](https://developer.peopleforce.io/docs/authentication)).

```bash
# в .env: PEOPLEFORCE_API_KEY, DATABASE_URL
python -m src.peopleforce.sync --init-db
python -m src.peopleforce.sync
python -m src.peopleforce.sync --only employees
# опционально дублировать ответы API в peopleforce_raw.entity_record
python -m src.peopleforce.sync --raw
# только сырой слой (без витрины): peopleforce_raw.entity_record (см. src/peopleforce/bulk_endpoints.py)
python -m src.peopleforce.sync --all-raw
# то же: python -m src.peopleforce.bulk_raw
```

Сырой JSON одного сотрудника из API (сверка полей): `python scripts/dump_peopleforce_employee_sample.py`

Запись в **PeopleForce API** (создать/обновить сотрудника, чтобы **они** прислали вебхук вам; не путать с проверкой своего URL): `python scripts/peopleforce_api_employee_write.py --help` (тот же `PEOPLEFORCE_API_KEY`). Убрать сотрудника в PeopleForce «как сможет» API: `python scripts/peopleforce_api_employee_write.py remove --id <id>` (сначала пробуется `DELETE`, обычно — fallback с `POST …/terminate`; см. `termination-refs`).

Проверка приёмника вебхуков (POST + сырой JSON, опционально подпись; PUT/DELETE покажут 405): `python scripts/test_peopleforce_webhook_http.py` (см. `--help`). URL по умолчанию: `PEOPLEFORCE_WEBHOOK_TEST_URL`, иначе **тот же хост, что** `WEBHOOK_PUBLIC_URL`, путь `/peopleforce/webhook`, иначе `http://127.0.0.1:8000/peopleforce/webhook`.

**Webhooks** PeopleForce: обзор топиков — [Webhooks overview](https://developer.peopleforce.io/docs/webhooks), настройка URL и подписи — [Starting with webhooks](https://developer.peopleforce.io/docs/starting-with-webhooks). В UI: **Settings → Webhooks → Add new webhook**; **Payload URL** — `https://<хост>/peopleforce/webhook` (локально: `uvicorn src.webhook_app:app --host 0.0.0.0 --port 8000`, `requirements-webhook.txt`). **Secret** → **`PEOPLEFORCE_WEBHOOK_SECRET`**, проверка заголовка **`x-peopleforce-signature`** (HMAC-SHA256, формат `sha256=` + hex). **Topics**: при необходимости отметьте `employee_*`, `leave_request_*`, `applicant_*`, `vacancy_*`, `vacancy_application_*` и т.д. (см. таблицу в доке). Код: `src/peopleforce/webhook_dispatch.py` — маршрутизация по `action` в `peopleforce_dm` и `peopleforce_raw.entity_record`. Тесты: `python -m pytest tests/test_peopleforce_webhook.py -v`.

## Инициализация БД и первая загрузка

```bash
python -m src.sync --init-db
python -m src.sync
```

Одна сущность:

```bash
python -m src.sync --only deals
```

## Модель данных

**Витрина `pipedrive_dm`** (основной слой для BI): `person`, `organization`, `deal`, `activity`, `lead`, `product`, `note`, `call_log`, `file`, `project`, `pipedrive_user`; плюс **`custom_field_value`** — EAV для кастомных полей из `*Fields` (`field_key`, человекочитаемое `field_name`, `value_text` / `value_json`). Источник строк — типовые `GET /v1/{deals,persons,...}` + метаданные полей `GET /v1/{deal,person,...}Fields`.

**Сырой слой `pipedrive_raw`**: `field_definition` — каталог полей; `entity_record` — **полный JSON** каждой выгруженной строки API. Для сущностей, у которых есть таблицы в `pipedrive_dm`, данные дублируются: витрина для BI + `entity_record` как полная копия ответа (и `custom_resolved` по `*Fields`, где применимо).

Применение схем: `python -m src.sync --schema-only` (файлы `sql/001` … `004`, `006`, `007`, `008`).

### Соответствие перечню ТЗ §2.1 (сущности)

Все позиции из ТЗ заведены в `src/entities.py` (`ENTITY_SPECS`). Синк `python -m src.sync` обходит их по очереди.

| Перечень ТЗ | Имя в коде / `entity_type` в `entity_record` |
|-------------|-----------------------------------------------|
| Deals, Persons, Organizations, Activities, Leads, Products, Notes, CallLogs, Files | `deals`, `persons`, … |
| Pipelines, Stages, ActivityTypes, LeadLabels, LeadSources, Currencies | `pipelines`, `stages`, … |
| *Fields (Deal/Person/Org/Product/Activity/Lead/Note/Project) | загрузка каталога в `pipedrive_raw.field_definition` + дубли строк в `entity_record` для `*_fields`; у сущностей с витриной также `GET …Fields` при старте синка этой сущности |
| Users, Roles, LegacyTeams, UserConnections, UserSettings | `users`, `roles`, … |
| Projects, ProjectTemplates, Meetings, Goals, Filters, Channels | `projects`, … |
| DealProducts, OrganizationRelationships | `deal_products`, `organization_relationships` |

**Полнота строк:** для списков сущностей используется пагинация `start` / `limit` до исчерпания `more_items_in_collection` (`PipedriveClient.iter_collection`). Для **всех** определений кастомных полей (*Fields) пагинация такая же — см. `_load_field_rows` в `src/sync.py` (не одна страница).

**Webhooks (обновления почти в реальном времени):** HTTP‑приёмник `POST /webhook` в `src/webhook_app.py` (uvicorn, порт **8000**). Зависимости: `pip install -r requirements-webhook.txt`. Деплой: `docker compose -f docker-compose.webhook.yml up -d --build` (те же переменные, что у синка, плюс опционально `WEBHOOK_SECRET`, `WEBHOOK_PUBLIC_URL` для `scripts/register_pipedrive_webhooks.py`). Полный снимок данных по-прежнему даёт запуск `python -m src.sync`. Проверка цепочки «POST/PUT в Pipedrive → событие на webhook»: на сервере `docker compose -f docker-compose.webhook.yml logs -f`, локально `python scripts/pipedrive_create_update_deal.py` (создаёт сделку и правит её; в логах — два вызова `/webhook`; см. docstring скрипта).

**Примеры SQL под BI (ТЗ §2.2):** [sql/bi_example_queries.sql](sql/bi_example_queries.sql).

### Почему часть таблиц «пустые»

- **`pipedrive_dm.call_log` и счётчик `call_logs: 0`:** эндпоинт [GET /v1/callLogs](https://developers.pipedrive.com/docs/api/v1/CallLogs) отдаёт только **логи звонков интегрированной телефонии** для контекста пользователя API; без такой интеграции или звонков ответ будет пустым. Часть звонков в CRM идёт как **активности с типом `call`** — они уже в **`pipedrive_dm.activity`**; для BI добавлено представление **`pipedrive_dm.v_phone_calls_from_activities`**.
- **`pipedrive_dm` vs `pipedrive_raw`:** часть справочников нормализована (`pipeline`, `stage`, `currency`, `deal_product_line`); остальное из перечня ТЗ — в **`pipedrive_raw.entity_record`** (полный JSON) и/или **`field_definition`**.

## BlazeSQL (BI)

Пошаговое подключение к этой PostgreSQL: **[docs/blazesql.md](docs/blazesql.md)** — хост/порт, whitelist IP **`35.192.145.209`** для веб-версии, схемы **`pipedrive_dm`** / **`pipedrive_raw`**, опциональная роль только на чтение (`sql/005_blazesql_readonly_role.sql`). Официальный гайд: [Connecting Your Database](https://help.blazesql.com/en/articles/13586274-connecting-your-database).

## Ограничения и следующие шаги

- Часть эндпоинтов в вашем аккаунте может отличаться или требовать других прав — при `404` сверьте путь с актуальной документацией.
- Плановый синк: **полная перезагрузка** по сущности при каждом запуске (upsert по `id`). Дополнительно webhooks подтягивают изменения по событиям; инкремент только по `update_time` вместо полного списка и отдельный шедулер — при необходимости следующий этап.
- Подключение BlazeSQL к PostgreSQL и дашборды выполняются в самом BlazeSQL; для качества NL‑запросов обычно добавляют представления (`VIEW`) с понятными именами колонок.

## Сложность реализации (по ТЗ целиком)

| Блок | Оценка | Комментарий |
|------|--------|-------------|
| PostgreSQL + схема | Низкая | Стандартный деплой |
| Выгрузка API + пагинация | Средняя | Много сущностей, лимиты, идемпотентность |
| Кастомные поля | Средняя | Уже заложено: `*Fields` + `custom_resolved` |
| Надёжность (ретраи, инкремент) | Средняя | Доработка после MVP |
| BlazeSQL + примеры | Низкая–средняя | В основном настройка продукта, не код |
| NLQ «без семантики» | Зависит от BI | Часто нужны витрины / описание полей |

Итого: **реализуемо** как типовой ELT; основное время уходит на полноту сущностей, стабильность синка и подготовку данных под вопросы пользователей, а не на «магию» интеграции.
