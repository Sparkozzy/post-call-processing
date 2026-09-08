import os
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    SUPABASE_MASTER_URL: str
    SUPABASE_MASTER_SERVICE_KEY: str
    RETELL_API_KEY: str = ""
    ENVIRONMENT: str = "development"
    PORT: int = 8000

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )


settings = Settings()
