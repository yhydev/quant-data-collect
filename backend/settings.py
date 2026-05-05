"""Application settings powered by Dynaconf."""

from __future__ import annotations

import os

try:
    from dynaconf import Dynaconf
except ModuleNotFoundError:  # pragma: no cover
    Dynaconf = None


class _FallbackSettings:
    def __init__(self) -> None:
        self._defaults = {
            "log_level": "INFO",
            "collector_type": "mock",
            "trader_type": "mock",
            "order_plugin_name": "futures_first",
            "cors_origins": ["http://localhost:3000", "http://localhost:8000"],
            "database": {
                "host": "localhost",
                "port": 5432,
                "user": "postgres",
                "password": "postgres",
                "name": "arbitrage",
                "pool_size": 5,
                "max_overflow": 10,
            },
            "binance": {
                "api_key": "",
                "secret_key": "",
                "testnet": False,
                "auth_http_proxy": "",
            },
        }

    def get(self, key: str, default=None):
        if key == "database.host":
            return os.getenv("POSTGRES_HOST", self._defaults["database"]["host"])
        if key == "database.port":
            return int(os.getenv("POSTGRES_PORT", str(self._defaults["database"]["port"])))
        if key == "database.user":
            return os.getenv("POSTGRES_USER", self._defaults["database"]["user"])
        if key == "database.password":
            return os.getenv("POSTGRES_PASSWORD", self._defaults["database"]["password"])
        if key == "database.name":
            return os.getenv("POSTGRES_DB", self._defaults["database"]["name"])
        if key == "database.pool_size":
            return int(os.getenv("DB_POOL_SIZE", str(self._defaults["database"]["pool_size"])))
        if key == "database.max_overflow":
            return int(os.getenv("DB_MAX_OVERFLOW", str(self._defaults["database"]["max_overflow"])))
        if key == "binance.api_key":
            return os.getenv("BINANCE_API_KEY", self._defaults["binance"]["api_key"])
        if key == "binance.secret_key":
            return os.getenv("BINANCE_SECRET_KEY", self._defaults["binance"]["secret_key"])
        if key == "binance.testnet":
            raw = os.getenv("BINANCE_TESTNET", str(self._defaults["binance"]["testnet"]))
            return str(raw).lower() in ("1", "true", "yes", "on")
        if key == "binance.auth_http_proxy":
            return os.getenv(
                "BINANCE_AUTH_HTTP_PROXY",
                os.getenv("BINANCE_HTTP_PROXY", os.getenv("HTTP_PROXY", self._defaults["binance"]["auth_http_proxy"])),
            )
        if key == "cors_origins":
            raw = os.getenv("CORS_ORIGINS")
            if raw:
                return [item.strip() for item in raw.split(",") if item.strip()]
            return self._defaults["cors_origins"]
        if key == "collector_type":
            return os.getenv("COLLECTOR_TYPE", self._defaults["collector_type"])
        if key == "trader_type":
            return os.getenv("TRADER_TYPE", self._defaults["trader_type"])
        if key == "order_plugin_name":
            return os.getenv("ORDER_PLUGIN_NAME", self._defaults["order_plugin_name"])
        if key == "log_level":
            return os.getenv("LOG_LEVEL", self._defaults["log_level"])
        return self._defaults.get(key, default)


if Dynaconf is None:  # pragma: no cover
    settings = _FallbackSettings()
else:
    settings = Dynaconf(
        envvar_prefix="APP",
        settings_files=["settings.toml", ".secrets.toml"],
        environments=True,
        load_dotenv=True,
    )
