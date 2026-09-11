"""Заморозка 2026-09-07: зависший cleanup под Semaphore(1) вешал оба планировщика.

Проверяем, что ни один зависший вызов Playwright не держит семафор дольше таймаута.
"""
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src import browser_pool


@pytest.fixture(autouse=True)
def fast_timeouts(monkeypatch, tmp_path):
    monkeypatch.setattr(browser_pool.settings, "sessions_dir", str(tmp_path))
    monkeypatch.setattr(browser_pool, "_semaphore", asyncio.Semaphore(1))
    monkeypatch.setattr(browser_pool, "CLEANUP_TIMEOUT_S", 0.05)
    monkeypatch.setattr(browser_pool, "RESTART_TIMEOUT_S", 0.05)
    yield


def _hanging_context():
    ctx = MagicMock()

    async def never():
        await asyncio.Event().wait()

    ctx.close = MagicMock(side_effect=lambda: never())
    ctx.storage_state = MagicMock(side_effect=lambda **kw: never())
    return ctx


@pytest.mark.asyncio
async def test_acquire_releases_semaphore_when_close_hangs():
    """Зависший context.close() не блокирует следующий acquire."""
    ctx = _hanging_context()

    with patch("src.browser_pool.get_context", AsyncMock(return_value=ctx)):
        async with browser_pool.acquire("1", save_on_exit=False):
            pass

        # Семафор свободен — второй заход проходит без ожидания.
        await asyncio.wait_for(
            browser_pool.acquire("1", save_on_exit=False).__aenter__(), timeout=1.0
        )
    assert browser_pool._semaphore._value >= 0


@pytest.mark.asyncio
async def test_acquire_releases_semaphore_when_storage_state_hangs():
    """Зависший storage_state() тоже не держит семафор."""
    ctx = _hanging_context()

    with patch("src.browser_pool.get_context", AsyncMock(return_value=ctx)):
        await asyncio.wait_for(_use_pool("2", save=True), timeout=1.0)


async def _use_pool(cid, save):
    async with browser_pool.acquire(cid, save_on_exit=save):
        pass


@pytest.mark.asyncio
async def test_two_sequential_acquires_after_hang():
    """После зависшего cleanup следующий прогон работает штатно."""
    hanging = _hanging_context()
    good = MagicMock()
    good.close = AsyncMock()
    good.storage_state = AsyncMock()

    contexts = [hanging, good]
    with patch("src.browser_pool.get_context", AsyncMock(side_effect=contexts)):
        await asyncio.wait_for(_use_pool("3", save=True), timeout=1.0)
        await asyncio.wait_for(_use_pool("3", save=True), timeout=1.0)

    good.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_hanging_cleanup_marks_browser_dirty():
    """После таймаута cleanup браузер помечен битым — get_browser поднимет новый."""
    ctx = _hanging_context()
    with patch("src.browser_pool.get_context", AsyncMock(return_value=ctx)):
        await asyncio.wait_for(_use_pool("4", save=False), timeout=1.0)

    assert browser_pool._browser_dirty is True


@pytest.mark.asyncio
async def test_restart_survives_hanging_browser_close(monkeypatch):
    """restart() не виснет, если browser.close() не отвечает."""

    async def never():
        await asyncio.Event().wait()

    browser = MagicMock()
    browser.is_connected = MagicMock(return_value=True)
    browser.close = MagicMock(side_effect=lambda: never())

    pw = MagicMock()
    pw.stop = MagicMock(side_effect=lambda: never())

    monkeypatch.setattr(browser_pool, "_browser", browser)
    monkeypatch.setattr(browser_pool, "_playwright", pw)

    with patch("src.browser_pool.get_browser", AsyncMock(return_value=MagicMock())):
        await asyncio.wait_for(browser_pool.restart(), timeout=1.0)


@pytest.mark.asyncio
async def test_get_browser_recreates_when_dirty(monkeypatch):
    """Битый браузер пересоздаётся, старый не закрывается синхронно."""
    old = MagicMock()
    old.is_connected = MagicMock(return_value=True)
    old.close = AsyncMock()
    monkeypatch.setattr(browser_pool, "_browser", old)
    monkeypatch.setattr(browser_pool, "_browser_dirty", True)

    new_browser = MagicMock()
    fake_chromium = MagicMock()
    fake_chromium.launch = AsyncMock(return_value=new_browser)
    fake_pw = MagicMock()
    fake_pw.chromium = fake_chromium
    monkeypatch.setattr(browser_pool, "_playwright", fake_pw)

    got = await asyncio.wait_for(browser_pool.get_browser(), timeout=1.0)

    assert got is new_browser
    assert browser_pool._browser_dirty is False
