"""Заморозка 2026-09-07: зависший прогон убивал планировщик молча, без ERROR.

Watchdog ограничивает прогон по времени и всегда возвращает управление циклу.
"""
import asyncio

import pytest

from src import main as main_mod


@pytest.mark.asyncio
async def test_run_guarded_returns_on_hang(caplog):
    """Зависший прогон прерывается по таймауту, цикл продолжает жить."""

    async def never():
        await asyncio.Event().wait()

    with caplog.at_level("ERROR"):
        await asyncio.wait_for(
            main_mod._run_guarded(never, "scrape", timeout=0.05), timeout=1.0
        )

    assert any("завис" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_run_guarded_passes_through_success():
    """Штатный прогон отрабатывает целиком."""
    calls = []

    async def work():
        calls.append(1)

    await main_mod._run_guarded(work, "messages", timeout=1.0)
    assert calls == [1]


@pytest.mark.asyncio
async def test_run_guarded_logs_exception(caplog):
    """Исключение прогона логируется, наружу не летит."""

    async def boom():
        raise RuntimeError("bang")

    with caplog.at_level("ERROR"):
        await main_mod._run_guarded(boom, "scrape", timeout=1.0)

    assert any("bang" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_run_guarded_cancels_hung_task():
    """Зависшая задача получает cancel, а не остаётся жить вечно."""
    started = asyncio.Event()
    cancelled = asyncio.Event()

    async def slow():
        started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.set()
            raise

    await main_mod._run_guarded(slow, "scrape", timeout=0.05)
    await asyncio.sleep(0.01)

    assert started.is_set()
    assert cancelled.is_set()


def test_watchdog_timeouts_are_sane():
    """Парсинг длинный (десятки минут), inbox короткий — лимиты не должны рубить норму."""
    assert main_mod.SCRAPE_WATCHDOG_S >= 45 * 60
    assert main_mod.MESSAGES_WATCHDOG_S >= 10 * 60
    assert main_mod.MESSAGES_WATCHDOG_S < main_mod.SCRAPE_WATCHDOG_S
