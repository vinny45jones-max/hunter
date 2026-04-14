import logging
from typing import List

from dotenv import load_dotenv
from pydantic_settings import BaseSettings, SettingsConfigDict

# Загрузить .env с override=True ПЕРЕД созданием Settings,
# чтобы .env всегда побеждал пустые системные env vars
load_dotenv(override=True)


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # Telegram
    telegram_bot_token: str

    # Claude API
    anthropic_api_key: str

    # Шифрование паролей юзеров (Fernet base64 key)
    fernet_key: str

    # rabota.by
    rabota_email: str
    rabota_password: str

    # Поиск
    search_queries: str = "директор,CEO,AI"
    search_city: str = "Минск"
    search_area_id: int = 16  # rabota.by area code: 16=Минск, 1002=Беларусь
    min_relevance_score: int = 50
    scrape_interval_minutes: int = 30
    message_check_interval_minutes: int = 5
    max_pages: int = 1

    # Пути (Railway Volume монтируется в /data)
    db_path: str = "/data/hunter.db"
    session_path: str = "/data/rabota_session.json"
    sessions_dir: str = "/data/sessions"

    # Профиль кандидата
    candidate_profile_path: str = "profile.yml"

    # Лимиты
    max_applies_per_day: int = 10

    @property
    def search_keywords(self) -> List[str]:
        return [q.strip() for q in self.search_queries.split(",")]


settings = Settings()

# Логгирование
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("hunter")
