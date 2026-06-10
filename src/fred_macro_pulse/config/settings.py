from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    fred_api_key: str
    db_path: str = "data/fred_macro.duckdb"
    log_level: str = "INFO"
    fred_base_url: str = "https://api.stlouisfed.org/fred"
    max_concurrent_requests: int = 10


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
