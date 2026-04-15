import asyncio
import os
import signal
from datetime import datetime

import aiosqlite
import yaml
from apscheduler.schedulers.asyncio import AsyncIOScheduler

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

    # 2. Scheduler
    scheduler = AsyncIOScheduler()

    scheduler.add_job(
        pipeline.run_pipeline,
        "cron",
        hour=8,
        minute=0,
        id="vacancy_pipeline",
        name="Vacancy Pipeline",
    )

    scheduler.add_job(
        pipeline.check_messages,
        "cron",
        hour=8,
        minute=0,
        id="check_messages",
        name="Check Messages",
    )

    scheduler.start()
    log.info("Scheduler started: vacancies and messages daily at 08:00")

    # 3. Telegram bot
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

    # Keep running until signal
    try:
        await stop_event.wait()
    except (KeyboardInterrupt, SystemExit):
        pass

    # Cleanup
    log.info("Shutting down...")
    scheduler.shutdown(wait=False)
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
