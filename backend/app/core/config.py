from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    database_url: str
    secret_key: str
    environment: str = "development"
    sync_interval_minutes: int = 60
    algorithm: str = "HS256"
    access_token_expire_minutes: int = 480  # 8 horas

    # Rutas archivos Excel (montados via Docker volume)
    path_flujos: str = "/data/sources/FLUJOS_DE_EFECTIVO.xlsx"
    path_ov_cartera: str = "/data/sources/OV_CARTERA_DESIST_METAS.xlsx"
    path_inventario: str = "/data/sources/VERSION_FINAL_INVENTARIOS_LOTES_V2.xlsx"

    class Config:
        env_file = ".env"
        case_sensitive = False


@lru_cache()
def get_settings() -> Settings:
    return Settings()
