# Rabota Hunter

Автоматический поиск вакансий на rabota.by с AI-фильтрацией и Telegram-ботом.

## Архитектура

Python 3.11, async. Модули в `src/`:

- `main.py` — точка входа: init DB, бэкфилл профиля, Telegram polling + фоновые asyncio-loop'ы (парсинг ежедневно в `scrape_hour`:00 по `timezone`, входящие каждые `message_check_interval_minutes`)
- `pipeline.py` — основной цикл: scrape -> deduplicate -> AI score -> notify TG
- `scraper.py` — Playwright-скрапер rabota.by (поиск + описание вакансий)
- `ai_filter.py` — Claude API (оценка+cover letter в одном вызове, ответы)
- `bot.py` — Telegram-бот (команды, inline-кнопки, ConversationHandler, онбординг)
- `database.py` — SQLite через aiosqlite (vacancies, messages, conversations, search_log, users)
- `auth.py` — логин на rabota.by, storage_state per-user, ретраи
- `browser_pool.py` — пул браузерных контекстов Playwright (семафор, per-user сессии)
- `crypto.py` — Fernet-шифрование учёток (encrypt/decrypt, ключ `FERNET_KEY`)
- `inbox.py` — проверка входящих сообщений на rabota.by
- `applier.py` — автоматический отклик на вакансию через браузер
- `responder.py` — отправка ответов в переписки на rabota.by
- `cover_flow.py` — извлечение требований к cover letter из описания вакансии
- `config.py` — pydantic-settings, все параметры из `.env`
- `models.py` — dataclasses: Vacancy, Message, Conversation
- `resume_parser.py` — парсинг PDF/DOCX резюме + Claude API → CandidateProfile (имя, навыки, ключевые слова)

## Стек

- `playwright` — браузерная автоматизация (Chromium, headless)
- `python-telegram-bot` — Telegram Bot API
- `anthropic` — Claude API (модель: claude-sonnet-4-20250514)
- `aiosqlite` — async SQLite
- `pydantic-settings` — конфигурация из env
- `cryptography` — Fernet-шифрование учёток
- `pdfplumber` — парсинг PDF-резюме
- `python-docx` — парсинг DOCX-резюме

## Запуск

```bash
# Локально
python -m src.main

# Docker
docker compose up -d

# Деплой — Railway (Dockerfile, volume /data)
```

## Тесты

```bash
# Локальные тесты (156 passed, 2 skipped)
python -m pytest -q

# Live API тесты (нужен ANTHROPIC_API_KEY в .env)
python -m pytest tests/test_ai_filter_live.py --live-api -q

# Live end-to-end (API + браузер)
python -m pytest tests/test_pipeline_live.py --live-api --live-web -q
```

Live-тесты отключены по умолчанию (флаги `--live-api`, `--live-web`).

## Конфигурация

Все параметры в `.env` (см. `src/config.py`):
- `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID` — Telegram
- `ANTHROPIC_API_KEY` — Claude API
- `RABOTA_EMAIL`, `RABOTA_PASSWORD` — учётка rabota.by
- `SEARCH_QUERIES` — ключевые слова через запятую
- `MIN_RELEVANCE_SCORE` — порог релевантности (0-100)
- `DB_PATH`, `SESSION_PATH` — пути к данным

## Важно

- `.env` содержит секреты — никогда не коммитить
- CSS-селекторы rabota.by в `scraper.py:SELECTORS` и `inbox.py:INBOX_SELECTORS` — могут сломаться при обновлении сайта
- На Windows скрапер использует системный Chrome (`chrome` channel), в Docker — Playwright Chromium
- Профиль кандидата формируется динамически из резюме через `resume_parser.py` (CandidateProfile); базовые промпты в `ai_filter.py` (EVALUATE_PROMPT, COVER_LETTER_PROMPT, REPLY_PROMPT)
- Учётки rabota.by шифруются Fernet (`crypto.py`), ключ `FERNET_KEY` в `.env`
- Лимит откликов: `max_applies_per_day` (default 10)
