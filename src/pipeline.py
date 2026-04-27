import asyncio
import random

from src.config import settings, log
from src import database, scraper, ai_filter, bot, inbox, browser_pool, auth

# Задержки между попытками логина при сетевой ошибке (сек). Длина = число попыток.
LOGIN_RETRY_DELAYS = (0, 30, 60, 90)

# Стоп-слова — вакансии с этими словами в названии отсекаются сразу
STOP_WORDS = [
    "магазин", "школ", "детский сад", "аптек", "медсестр", "повар",
    "кассир", "уборщ", "охранник", "водитель", "грузчик", "сторож",
    "воспитатель", "учитель", "преподаватель", "фармацевт", "санитар",
    "медицинск", "стоматолог", "ветеринар", "парикмахер", "маникюр",
    "продавец", "кладовщик", "слесарь", "сварщик", "электрик",
    "сантехник", "плотник", "токар", "фрезеровщик",
]

# Порог быстрой оценки — вакансии выше него идут на полную проверку
BATCH_THRESHOLD = 40


def _passes_stop_words(title: str) -> bool:
    title_lower = title.lower()
    return not any(word in title_lower for word in STOP_WORDS)


async def run_pipeline_for_user(chat_id: str):
    """Запуск пайплайна для одного юзера."""
    log.info(f"Pipeline [{chat_id}]: starting vacancy scan...")

    try:
        await bot.send_text(chat_id, "🔍 Начинаю поиск вакансий...")

        # 0. Логин (с ретраем на сетевую ошибку: пауза + рестарт браузера + повтор)
        total_attempts = len(LOGIN_RETRY_DELAYS)
        for attempt, delay in enumerate(LOGIN_RETRY_DELAYS):
            if delay:
                log.info(f"Pipeline [{chat_id}]: ждём {delay}с перед попыткой {attempt + 1}/{total_attempts}")
                await asyncio.sleep(delay)
            try:
                async with browser_pool.acquire(chat_id) as ctx:
                    await auth.ensure_logged_in(ctx, chat_id)
                break
            except auth.LoginError as e:
                log.warning(f"Pipeline [{chat_id}]: login failed: {e}")
                await bot.send_text(
                    chat_id,
                    "❌ Не смог войти в rabota.by. Проверь креды в /settings.",
                )
                return
            except Exception as e:
                is_last = attempt == total_attempts - 1
                if not is_last and browser_pool.is_network_error(e):
                    log.warning(
                        f"Pipeline [{chat_id}]: network error on login "
                        f"(попытка {attempt + 1}/{total_attempts}), restarting browser: {e}"
                    )
                    await browser_pool.restart()
                    continue
                raise

        # 1. Настройки юзера
        user_queries = await database.get_setting(chat_id, "search_queries", settings.search_queries)
        user_keywords = [q.strip() for q in user_queries.split(",") if q.strip()]
        user_max_pages = await database.get_setting_int(chat_id, "max_pages", settings.max_pages)

        # 2. Скрапинг
        all_vacancies = await scraper.parse_all_keywords(
            chat_id, keywords=user_keywords, max_pages=user_max_pages
        )

        # 3. Дедупликация
        new_vacancies = await database.filter_new(all_vacancies)
        if not new_vacancies:
            log.info(f"Pipeline [{chat_id}]: no new vacancies")
            await bot.send_text(chat_id, "Новых вакансий не найдено.")
            return

        log.info(f"Pipeline [{chat_id}]: {len(new_vacancies)} new vacancies found")

        # 3. Стоп-слова фильтр
        before_stop = len(new_vacancies)
        new_vacancies = [v for v in new_vacancies if _passes_stop_words(v.title)]
        filtered_out = before_stop - len(new_vacancies)
        if filtered_out:
            log.info(f"Pipeline [{chat_id}]: стоп-слова отсекли {filtered_out} вакансий")

        if not new_vacancies:
            await bot.send_text(chat_id, "Новых подходящих вакансий не найдено (все отсечены стоп-словами).")
            return

        await bot.send_text(
            chat_id,
            f"Найдено {len(all_vacancies)} вакансий, {len(new_vacancies)} новых "
            f"(отсечено стоп-словами: {filtered_out}). Быстрая оценка..."
        )

        # 4. Батч-оценка по названиям (быстрая, без описаний)
        try:
            batch_scores = await ai_filter.batch_evaluate_titles(new_vacancies, chat_id)
        except Exception as e:
            log.error(f"Pipeline [{chat_id}]: батч-оценка упала: {e}")
            await bot.send_text(chat_id, f"⚠️ Ошибка AI при быстрой оценке: {e}")
            return
        promising = []
        for i, v in enumerate(new_vacancies):
            score = batch_scores.get(i, 0)
            v.relevance_score = score
            if score >= BATCH_THRESHOLD:
                promising.append(v)
            else:
                v.relevance_reason = f"Быстрая оценка: {score}"
                await database.save_vacancy(v)

        log.info(f"Pipeline [{chat_id}]: батч-оценка — {len(promising)} перспективных из {len(new_vacancies)}")

        if not promising:
            await bot.send_text(chat_id, "После быстрой оценки подходящих вакансий не найдено.")
            return

        await bot.send_text(chat_id, f"Перспективных: {len(promising)}. Загружаю описания...")

        # 5. Загрузить описания только для перспективных
        for v in promising:
            try:
                v.description = await scraper.get_full_description(v.url, chat_id)
                await asyncio.sleep(random.uniform(1, 2))
            except Exception as e:
                log.warning(f"Pipeline [{chat_id}]: failed to get description for {v.url}: {e}")

        # 6. Полная AI-оценка с описанием + cover letter (один вызов)
        min_score = await database.get_setting_int(chat_id, "min_relevance_score", settings.min_relevance_score)
        await bot.send_text(chat_id, f"Оцениваю {len(promising)} вакансий с описаниями...")
        for v in promising:
            try:
                result = await ai_filter.evaluate_and_cover(v, chat_id, min_score)
                v.relevance_score = result["score"]
                v.relevance_reason = result["reason"]

                if v.relevance_score >= min_score and result.get("cover_letter"):
                    v.cover_letter = result["cover_letter"]
                    v.status = "filtered"
            except Exception as e:
                log.warning(f"Pipeline [{chat_id}]: AI filter error for {v.title}: {e}")
                await bot.send_text(chat_id, f"⚠️ Ошибка AI для «{v.title}»: {type(e).__name__}")

            await database.save_vacancy(v)

        # 7. Отправить в Telegram
        relevant = [v for v in promising if v.status == "filtered"]
        for v in relevant:
            try:
                await bot.send_vacancy_card(chat_id, v)
                await database.update_status(v.id, "sent_to_tg")
            except Exception as e:
                log.error(f"Pipeline [{chat_id}]: TG send error for {v.title}: {e}")

        # 8. Лог
        await database.save_search_log(
            query=",".join(user_keywords),
            total=len(all_vacancies),
            new=len(new_vacancies),
            relevant=len(relevant),
        )

        log.info(
            f"Pipeline [{chat_id}]: done. "
            f"Total={len(all_vacancies)}, new={len(new_vacancies)}, "
            f"promising={len(promising)}, relevant={len(relevant)}"
        )
        await bot.send_text(
            chat_id,
            f"✅ Поиск завершён.\n"
            f"Всего: {len(all_vacancies)}, новых: {len(new_vacancies)}, "
            f"перспективных: {len(promising)}, подходящих: {len(relevant)}"
        )

    except Exception as e:
        log.error(f"Pipeline [{chat_id}] error: {e}")
        try:
            await bot.send_text(chat_id, f"⚠️ Ошибка пайплайна: {e}")
        except Exception:
            pass


async def run_pipeline():
    """Запуск пайплайна для всех зарегистрированных юзеров."""
    chats = await database.get_all_registered_chats()
    if not chats:
        log.info("Pipeline: нет зарегистрированных юзеров")
        return
    for chat_id in chats:
        await run_pipeline_for_user(chat_id)


async def check_messages_for_user(chat_id: str):
    """Проверка входящих сообщений для одного юзера."""
    log.info(f"Inbox [{chat_id}]: checking messages...")

    try:
        total_attempts = len(LOGIN_RETRY_DELAYS)
        for attempt, delay in enumerate(LOGIN_RETRY_DELAYS):
            if delay:
                log.info(f"Inbox [{chat_id}]: ждём {delay}с перед попыткой {attempt + 1}/{total_attempts}")
                await asyncio.sleep(delay)
            try:
                async with browser_pool.acquire(chat_id) as ctx:
                    await auth.ensure_logged_in(ctx, chat_id)
                break
            except auth.LoginError as e:
                log.warning(f"Inbox [{chat_id}]: login failed: {e}")
                await bot.send_text(
                    chat_id,
                    "❌ Не смог войти в rabota.by. Проверь креды в /settings.",
                )
                return
            except Exception as e:
                is_last = attempt == total_attempts - 1
                if not is_last and browser_pool.is_network_error(e):
                    log.warning(
                        f"Inbox [{chat_id}]: network error on login "
                        f"(попытка {attempt + 1}/{total_attempts}), restarting browser: {e}"
                    )
                    await browser_pool.restart()
                    continue
                raise

        new_messages = await inbox.check_inbox(chat_id)

        for msg in new_messages:
            try:
                vacancy = None
                if msg.company and msg.vacancy_title:
                    vacancy = await database.find_vacancy_by_company_title(
                        msg.company, msg.vacancy_title
                    )
                    if vacancy:
                        msg.vacancy_id = vacancy.id

                from src.models import Conversation
                conv = Conversation(
                    conversation_id=msg.conversation_id or msg.message_id,
                    vacancy_id=msg.vacancy_id,
                    vacancy_title=msg.vacancy_title,
                    company=msg.company,
                    status="active",
                )
                await database.save_conversation(conv)

                saved = await database.save_incoming_message(msg)
                if saved is None:
                    continue

                await bot.send_message_card(chat_id, msg, vacancy)

            except Exception as e:
                log.warning(f"Inbox [{chat_id}]: error processing message: {e}")
                continue

        if new_messages:
            log.info(f"Inbox [{chat_id}]: {len(new_messages)} new messages processed")
        else:
            log.info(f"Inbox [{chat_id}]: no new messages")

    except Exception as e:
        log.error(f"Inbox [{chat_id}] check error: {e}")


async def check_messages():
    """Проверка сообщений для всех зарегистрированных юзеров."""
    chats = await database.get_all_registered_chats()
    if not chats:
        return
    for chat_id in chats:
        await check_messages_for_user(chat_id)
