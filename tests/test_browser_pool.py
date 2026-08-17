"""Шаг 7 плана: unit-тесты для src.browser_pool (без реального Playwright)."""
import asyncio
import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src import browser_pool


def test_session_path_format(monkeypatch, tmp_path):
    monkeypatch.setattr(browser_pool.settings, "sessions_dir", str(tmp_path))
    p = browser_pool._session_path(42)
    assert p == os.path.join(str(tmp_path), "42", "storage_state.json")


def test_wipe_session_removes_folder(tmp_path, monkeypatch):
    monkeypatch.setattr(browser_pool.settings, "sessions_dir", str(tmp_path))
    folder = tmp_path / "5"
    folder.mkdir()
    (folder / "storage_state.json").write_text("{}")
    browser_pool.wipe_session(5)
    assert not folder.exists()


def test_wipe_session_missing_ok(tmp_path, monkeypatch):
    monkeypatch.setattr(browser_pool.settings, "sessions_dir", str(tmp_path))
    browser_pool.wipe_session("nope")  # не падает


@pytest.mark.asyncio
async def test_save_context_writes_file(tmp_path, monkeypatch):
    monkeypatch.setattr(browser_pool.settings, "sessions_dir", str(tmp_path))
    context = MagicMock()
    context.storage_state = AsyncMock()
    path = await browser_pool.save_context(context, "7")
    expected = os.path.join(str(tmp_path), "7", "storage_state.json")
    assert path == expected
    context.storage_state.assert_awaited_once_with(path=expected)
    assert os.path.isdir(os.path.dirname(expected))


@pytest.mark.asyncio
async def test_get_context_without_existing_state(tmp_path, monkeypatch):
    monkeypatch.setattr(browser_pool.settings, "sessions_dir", str(tmp_path))
    fake_browser = MagicMock()
    fake_browser.new_context = AsyncMock(return_value="CTX")
    with patch("src.browser_pool.get_browser", AsyncMock(return_value=fake_browser)):
        ctx = await browser_pool.get_context("8")
    assert ctx == "CTX"
    kwargs = fake_browser.new_context.call_args.kwargs
    assert kwargs["storage_state"] is None
    assert kwargs["user_agent"] == browser_pool.USER_AGENT


@pytest.mark.asyncio
async def test_get_context_with_existing_state(tmp_path, monkeypatch):
    monkeypatch.setattr(browser_pool.settings, "sessions_dir", str(tmp_path))
    folder = tmp_path / "9"
    folder.mkdir()
    state = folder / "storage_state.json"
    state.write_text("{}")

    fake_browser = MagicMock()
    fake_browser.new_context = AsyncMock(return_value="CTX")
    with patch("src.browser_pool.get_browser", AsyncMock(return_value=fake_browser)):
        await browser_pool.get_context("9")
    kwargs = fake_browser.new_context.call_args.kwargs
    assert kwargs["storage_state"] == str(state)


@pytest.mark.asyncio
async def test_acquire_saves_and_closes_on_exit(tmp_path, monkeypatch):
    monkeypatch.setattr(browser_pool.settings, "sessions_dir", str(tmp_path))
    context = MagicMock()
    context.close = AsyncMock()
    context.storage_state = AsyncMock()

    with patch("src.browser_pool.get_context", AsyncMock(return_value=context)):
        async with browser_pool.acquire("10") as c:
            assert c is context

    context.storage_state.assert_awaited_once()
    context.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_acquire_skips_save_when_disabled(tmp_path, monkeypatch):
    monkeypatch.setattr(browser_pool.settings, "sessions_dir", str(tmp_path))
    context = MagicMock()
    context.close = AsyncMock()
    context.storage_state = AsyncMock()

    with patch("src.browser_pool.get_context", AsyncMock(return_value=context)):
        async with browser_pool.acquire("11", save_on_exit=False):
            pass

    context.storage_state.assert_not_awaited()
    context.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_acquire_closes_context_even_on_error(tmp_path, monkeypatch):
    monkeypatch.setattr(browser_pool.settings, "sessions_dir", str(tmp_path))
    context = MagicMock()
    context.close = AsyncMock()
    context.storage_state = AsyncMock()

    with patch("src.browser_pool.get_context", AsyncMock(return_value=context)):
        with pytest.raises(RuntimeError):
            async with browser_pool.acquire("12", save_on_exit=False):
                raise RuntimeError("boom")

    context.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_semaphore_serialises_concurrent_acquires(tmp_path, monkeypatch):
    """Два параллельных acquire() не работают одновременно — семафор = 1."""
    monkeypatch.setattr(browser_pool.settings, "sessions_dir", str(tmp_path))
    # Свежий семафор, чтобы не зависеть от глобального состояния.
    monkeypatch.setattr(browser_pool, "_semaphore", asyncio.Semaphore(1))

    active = 0
    peak = 0

    async def fake_get_context(chat_id):
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        ctx = MagicMock()
        ctx.close = AsyncMock()
        ctx.storage_state = AsyncMock()
        return ctx

    async def worker(cid):
        async with browser_pool.acquire(cid, save_on_exit=False):
            await asyncio.sleep(0.05)
            nonlocal_dec()

    def nonlocal_dec():
        nonlocal active
        active -= 1

    with patch("src.browser_pool.get_context", side_effect=fake_get_context):
        await asyncio.gather(worker("a"), worker("b"), worker("c"))

    assert peak == 1


def test_is_network_error_covers_navigation_timeout():
    """Timeout goto через медленный прокси — сетевая ошибка, а не «нет данных»."""
    from playwright.async_api import TimeoutError as PlaywrightTimeoutError

    err = PlaywrightTimeoutError(
        'Page.goto: Timeout 30000ms exceeded.\n'
        'Call log:\n  - navigating to "https://rabota.by/", waiting until "domcontentloaded"'
    )
    assert browser_pool.is_network_error(err) is True
    assert browser_pool.is_network_error(Exception("Page.goto: Timeout 60000ms exceeded.")) is True


def test_is_network_error_ignores_unrelated():
    assert browser_pool.is_network_error(ValueError("bad selector")) is False
