import os
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    SUPABASE_MASTER_URL: str
    SUPABASE_MASTER_SERVICE_KEY: str
    PRE_CALL_PROCESSING_URL: str = "http://pre-call-processing:8000/trigger"
    CALL_PREDICT_URL: str = "http://call-predict:8000/webhook/predict"
    ENVIRONMENT: str = "development"
    PORT: int = 8000

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )


settings = Settings()

