import asyncio
import os
import signal
from datetime import datetime

import aiosqlite
import yaml

from src.config import settings, log
from src import database, pipeline, bot


async def _backfill_profile_from_yaml():
    """Для юзеров с rabota_email, но без candidate_name — залить профиль из profile.yml."""
    path = settings.candidate_profile_path
    if not os.path.exists(path):
        return
    try:
        with open(path, "r", encoding="utf-8") as f:
            profile = yaml.safe_load(f) or {}
    except Exception as e:
        log.warning(f"Backfill: не удалось прочитать {path}: {e}")
        return

    name = profile.get("candidate_name")
    summary = profile.get("candidate_profile")
    keywords = profile.get("search_keywords") or []
    if not name or not summary:
        return

    async with aiosqlite.connect(settings.db_path) as db:
        cur = await db.execute(
            "SELECT DISTINCT chat_id FROM user_settings WHERE key='rabota_email' "
            "AND chat_id NOT IN (SELECT chat_id FROM user_settings WHERE key='candidate_name')"
        )
        rows = await cur.fetchall()

    for (cid,) in rows:
        await database.set_setting(cid, "candidate_name", name)
        await database.set_setting(cid, "candidate_profile", summary)
        await database.set_setting(cid, "search_queries", ", ".join(keywords))
        log.info(f"Backfill: профиль залит для chat_id={cid}")


async def main():
    log.info("Starting Rabota Hunter Bot...")

    # 1. Init DB
    await database.init()

    # 1.1 Бэкфилл профиля из profile.yml для юзеров, прошедших онбординг до фикса
    await _backfill_profile_from_yaml()

    # 2. Telegram bot (пайплайн — по команде /search и по расписанию ниже)
    app = bot.create_app()

    log.info("Starting Telegram polling...")
    await app.initialize()
    # Команды меню не ставим глобально — только per-chat после онбординга
    await app.bot.delete_my_commands()
    await app.start()
    await app.updater.start_polling()

    log.info("Bot is running. Waiting for updates...")

    # 4. Graceful shutdown via signal
    stop_event = asyncio.Event()

    def _signal_handler():
        log.info("Received shutdown signal...")
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _signal_handler)
        except NotImplementedError:
            # Windows — signal handlers not supported in asyncio
            pass

    # 3. Расписание: ежедневный парсинг в фикс. час + интервальная проверка входящих.
    # Прерываемый sleep через stop_event — для чистого шатдауна.
    async def _interruptible_sleep(seconds: float) -> bool:
        """Спит до timeout или до stop_event. True если пора выходить."""
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=seconds)
            return True
        except asyncio.TimeoutError:
            return False

    async def _interval_loop(coro_fn, minutes: int, name: str):
        while not stop_event.is_set():
            if await _interruptible_sleep(minutes * 60):
                break
            try:
                log.info(f"Scheduler[{name}]: запуск")
                await coro_fn()
            except Exception as e:
                log.error(f"Scheduler[{name}] error: {e}")

    async def _daily_loop(coro_fn, hour: int, tz_name: str, name: str):
        from datetime import datetime, timedelta
        from zoneinfo import ZoneInfo
        tz = ZoneInfo(tz_name)
        while not stop_event.is_set():
            now = datetime.now(tz)
            nxt = now.replace(hour=hour, minute=0, second=0, microsecond=0)
            if nxt <= now:
                nxt += timedelta(days=1)
            wait = (nxt - now).total_seconds()
            log.info(f"Scheduler[{name}]: следующий запуск {nxt:%Y-%m-%d %H:%M} {tz_name}")
            if await _interruptible_sleep(wait):
                break
            try:
                log.info(f"Scheduler[{name}]: запуск")
                await coro_fn()
            except Exception as e:
                log.error(f"Scheduler[{name}] error: {e}")

    bg_tasks = [
        asyncio.create_task(
            _daily_loop(pipeline.run_pipeline, settings.scrape_hour, settings.timezone, "scrape")
        ),
        asyncio.create_task(
            _interval_loop(pipeline.check_messages, settings.message_check_interval_minutes, "messages")
        ),
    ]
    log.info(
        f"Scheduler: парсинг ежедневно в {settings.scrape_hour:02d}:00 ({settings.timezone}), "
        f"сообщения каждые {settings.message_check_interval_minutes} мин"
    )

    # Keep running until signal
    try:
        await stop_event.wait()
    except (KeyboardInterrupt, SystemExit):
        stop_event.set()

    # Cleanup
    log.info("Shutting down...")
    for t in bg_tasks:
        t.cancel()
    await asyncio.gather(*bg_tasks, return_exceptions=True)
    await app.updater.stop()
    await app.stop()
    await app.shutdown()
    from src import browser_pool
    await browser_pool.close()
    log.info("Shutdown complete")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
