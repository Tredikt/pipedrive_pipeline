"""Точка входа: синхронизация Pipedrive → PostgreSQL."""

from src.sync import __main__ as _cli

if __name__ == "__main__":
    _cli()
