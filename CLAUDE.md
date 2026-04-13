# Rabota Hunter

Автоматический поиск вакансий на rabota.by с AI-фильтрацией и Telegram-ботом.

## Архитектура

Python 3.11, async. Модули в `src/`:

- `main.py` — точка входа, APScheduler + Telegram polling
- `pipeline.py` — основной цикл: scrape -> deduplicate -> AI score -> notify TG
- `scraper.py` — Playwright-скрапер rabota.by (поиск + описание вакансий)
- `ai_filter.py` — Claude API (оценка релевантности, cover letter, ответы)
- `bot.py` — Telegram-бот (команды, inline-кнопки, ConversationHandler для ответов)
- `database.py` — SQLite через aiosqlite (vacancies, messages, conversations, search_log)
- `inbox.py` — проверка входящих сообщений на rabota.by
- `applier.py` — автоматический отклик на вакансию через браузер
- `responder.py` — отправка ответов в переписки на rabota.by
- `config.py` — pydantic-settings, все параметры из `.env`
- `models.py` — dataclasses: Vacancy, Message, Conversation

## Стек

- `playwright` — браузерная автоматизация (Chromium, headless)
- `python-telegram-bot` — Telegram Bot API
- `anthropic` — Claude API (модель: claude-sonnet-4-20250514)
- `apscheduler` — периодические задачи
- `aiosqlite` — async SQLite
- `pydantic-settings` — конфигурация из env

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
# Локальные тесты (102 passed, 3 skipped)
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
- Профиль кандидата захардкожен в `ai_filter.py` (промпты EVALUATE_PROMPT, COVER_LETTER_PROMPT, REPLY_PROMPT)
- Лимит откликов: `max_applies_per_day` (default 10)
