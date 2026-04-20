# PostgreSQL в Docker

Самодостаточная папка: можно скопировать только её на другую машину.

## Запуск

```powershell
cd postgres-docker
copy .env.example .env
docker compose up -d
```

Остановка: `docker compose down` (том с данными сохраняется).

## Подключение приложения (пайплайн в корне репозитория)

В **корневом** `.env` укажите `DATABASE_URL`, совпадающий с учёткой выше, например:

`postgresql://pipedrive:pipedrive@127.0.0.1:5432/pipedrive`

Порт и пароль берите из `postgres-docker/.env`. Для доступа **с другой машины** в `DATABASE_URL` укажите IP/домен сервера и тот же порт, что в `POSTGRES_PORT` (например `5436`).
