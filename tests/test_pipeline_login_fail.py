"""Пайплайн при провале auth.ensure_logged_in шлёт TG и выходит, не зовя scraper."""
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock

import pytest

from src import pipeline
from src.auth import LoginError


@asynccontextmanager
async def _fake_acquire(chat_id, save_on_exit=True):
    yield object()  # фиктивный контекст, ensure_logged_in его всё равно не использует (замокан)


@pytest.mark.asyncio
async def test_run_pipeline_for_user_login_fail(monkeypatch):
    send_progress = AsyncMock(return_value=123)
    update_progress = AsyncMock()
    ensure_logged_in = AsyncMock(side_effect=LoginError("bad creds"))
    parse_all_keywords = AsyncMock()

    monkeypatch.setattr(pipeline.bot, "send_progress", send_progress)
    monkeypatch.setattr(pipeline.bot, "update_progress", update_progress)
    monkeypatch.setattr(pipeline.browser_pool, "acquire", _fake_acquire)
    monkeypatch.setattr(pipeline.auth, "ensure_logged_in", ensure_logged_in)
    monkeypatch.setattr(pipeline.scraper, "parse_all_keywords", parse_all_keywords)
    # профиль задан — иначе гард выйдет до логина
    monkeypatch.setattr(pipeline.database, "get_setting", AsyncMock(return_value="Профиль есть"))
    monkeypatch.setattr(pipeline.database, "get_unsent_filtered", AsyncMock(return_value=[]))

    await pipeline.run_pipeline_for_user("42")

    ensure_logged_in.assert_awaited_once()
    parse_all_keywords.assert_not_awaited()
    # Старт — send_progress, уведомление об ошибке логина — update_progress
    send_progress.assert_awaited_once()
    update_progress.assert_awaited_once()
    last_msg = update_progress.await_args.args[3]
    assert "rabota.by" in last_msg
    assert "/settings" in last_msg


@pytest.mark.asyncio
async def test_check_messages_for_user_login_fail(monkeypatch):
    send_text = AsyncMock()
    ensure_logged_in = AsyncMock(side_effect=LoginError("bad creds"))
    check_inbox = AsyncMock()

    monkeypatch.setattr(pipeline.bot, "send_text", send_text)
    monkeypatch.setattr(pipeline.browser_pool, "acquire", _fake_acquire)
    monkeypatch.setattr(pipeline.auth, "ensure_logged_in", ensure_logged_in)
    monkeypatch.setattr(pipeline.inbox, "check_inbox", check_inbox)

    await pipeline.check_messages_for_user("42")

    ensure_logged_in.assert_awaited_once()
    check_inbox.assert_not_awaited()
    send_text.assert_awaited_once()
    assert "rabota.by" in send_text.await_args.args[1]


@pytest.mark.asyncio
async def test_run_pipeline_for_user_no_profile(monkeypatch):
    """Без candidate_profile пайплайн выходит до логина с понятным сообщением."""
    send_progress = AsyncMock(return_value=123)
    update_progress = AsyncMock()
    ensure_logged_in = AsyncMock()
    parse_all_keywords = AsyncMock()

    monkeypatch.setattr(pipeline.bot, "send_progress", send_progress)
    monkeypatch.setattr(pipeline.bot, "update_progress", update_progress)
    monkeypatch.setattr(pipeline.browser_pool, "acquire", _fake_acquire)
    monkeypatch.setattr(pipeline.auth, "ensure_logged_in", ensure_logged_in)
    monkeypatch.setattr(pipeline.scraper, "parse_all_keywords", parse_all_keywords)
    monkeypatch.setattr(pipeline.database, "get_setting", AsyncMock(return_value=None))

    await pipeline.run_pipeline_for_user("42")

    ensure_logged_in.assert_not_awaited()
    parse_all_keywords.assert_not_awaited()
    msg = update_progress.await_args.args[3]
    assert "Профиль не заполнен" in msg
    assert "/start" in msg


@pytest.mark.asyncio
async def test_run_pipeline_flushes_backlog(monkeypatch):
    """Недоставленные filtered-вакансии досылаются в начале /search и помечаются sent_to_tg."""
    from src.models import Vacancy

    send_progress = AsyncMock(return_value=123)
    update_progress = AsyncMock()
    send_vacancy_card = AsyncMock()
    update_status = AsyncMock()
    ensure_logged_in = AsyncMock(side_effect=LoginError("stop"))  # обрываем сразу после флаша

    backlog = [Vacancy(external_id="1", url="u", title="Коммерческий директор",
                       id=7, status="filtered", relevance_score=92)]

    monkeypatch.setattr(pipeline.bot, "send_progress", send_progress)
    monkeypatch.setattr(pipeline.bot, "update_progress", update_progress)
    monkeypatch.setattr(pipeline.bot, "send_vacancy_card", send_vacancy_card)
    monkeypatch.setattr(pipeline.browser_pool, "acquire", _fake_acquire)
    monkeypatch.setattr(pipeline.auth, "ensure_logged_in", ensure_logged_in)
    monkeypatch.setattr(pipeline.database, "get_setting", AsyncMock(return_value="Профиль есть"))
    monkeypatch.setattr(pipeline.database, "get_unsent_filtered", AsyncMock(return_value=backlog))
    monkeypatch.setattr(pipeline.database, "update_status", update_status)

    await pipeline.run_pipeline_for_user("42")

    send_vacancy_card.assert_awaited_once()
    assert send_vacancy_card.await_args.args[1].id == 7
    update_status.assert_awaited_once_with(7, "sent_to_tg")
