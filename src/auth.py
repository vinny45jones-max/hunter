"""Per-user авторизация rabota.by.

Хранит storage_state в `sessions_dir/{chat_id}/storage_state.json`.
Креды достаются из `user_settings` (зашифрованный пароль через Fernet).
Если в БД пусто — fallback на глобальные `settings.rabota_email`/`settings.rabota_password`.
"""
import os
from typing import Tuple

from playwright.async_api import BrowserContext, Page

from src.config import settings, log
from src import browser_pool, database


AUTH_MARKER_SELECTOR = (
    "a[href*='/applicant/resumes'], "
    "a[href*='/account/logout'], "
    "[data-qa='mainmenu_myResumes'], "
    "[data-qa='mainmenu_applicantResumes'], "
    ".applicant-sidebar"
)

LOGIN_URL = "https://rabota.by/account/login"


class LoginError(Exception):
    pass


async def _load_credentials(chat_id: str | int) -> Tuple[str, str]:
    email = await database.get_setting(str(chat_id), "rabota_email")
    password = await database.get_setting(str(chat_id), "rabota_password")
    if not email:
        email = settings.rabota_email
    if not password:
        password = settings.rabota_password
    if not email or not password:
        raise LoginError(f"no rabota credentials for chat_id={chat_id}")
    return email, password


async def _is_authorised(page: Page) -> bool:
    try:
        if "/account/login" in page.url:
            return False
        el = await page.query_selector(AUTH_MARKER_SELECTOR)
        if el is not None:
            return True
        # Фоллбэк: ищем текстовые маркеры в innerText body
        try:
            body = (await page.inner_text("body")).lower()
        except Exception:
            body = ""
        for marker in (
            "мой профиль",
            "мои резюме",
            "выйти",
            "откликнуться",
            "резюме и профиль",
            "отклики и приглашения",
            "ваша активность",
            "автопоиски",
            "избранные вакансии",
            "поднимите резюме",
        ):
            if marker in body:
                return True
        return False
    except Exception:
        return False


async def _dump_debug(page: Page, chat_id: str | int, tag: str) -> None:
    try:
        debug_dir = os.path.join(os.path.dirname(settings.db_path), "debug")
        os.makedirs(debug_dir, exist_ok=True)
        prefix = os.path.join(debug_dir, f"login_{chat_id}_{tag}")
        await page.screenshot(path=f"{prefix}.png", full_page=True)
        url = page.url
        title = await page.title()
        body_text = ""
        try:
            body_text = (await page.inner_text("body"))[:500]
        except Exception:
            pass
        log.warning(
            f"auth debug [{chat_id}/{tag}] url={url} title={title!r} "
            f"body_excerpt={body_text!r}"
        )
    except Exception as e:
        log.warning(f"auth._dump_debug failed: {e}")


async def _try_click(page: Page, selector: str, timeout: int = 3000) -> bool:
    """Клик по селектору с коротким таймаутом. Возвращает True если кликнулось."""
    try:
        await page.click(selector, timeout=timeout)
        return True
    except Exception:
        return False


async def _perform_login(page: Page, email: str, password: str) -> None:
    await page.goto(LOGIN_URL, wait_until="domcontentloaded", timeout=30000)
    await page.wait_for_timeout(1500)

    # 1. Опциональный role-picker "Я ищу работу" (на некоторых редизайнах отсутствует)
    if await _try_click(page, "text=Я ищу работу", timeout=3000):
        await page.wait_for_timeout(1000)

    # 2. Опциональная промежуточная кнопка "Войти"
    if await _try_click(page, 'button[data-qa="submit-button"]', timeout=3000):
        await page.wait_for_timeout(1500)

    # 3. Опциональный выбор способа "Почта"
    if await _try_click(page, "text=Почта", timeout=3000):
        await page.wait_for_timeout(1000)

    # 4. Email — обязательный шаг
    email_input = await page.wait_for_selector(
        '[data-qa="applicant-login-input-email"]', timeout=10000,
    )
    await email_input.fill(email)

    # 5. Опциональный переход на форму пароля
    if await _try_click(page, "text=Войти с паролем", timeout=3000):
        await page.wait_for_timeout(1500)

    # 6. Пароль
    password_input = await page.wait_for_selector(
        '[data-qa="applicant-login-input-password"]', timeout=5000,
    )
    await password_input.fill(password)

    # 7. Submit — Enter по полю пароля (надёжнее клика: на странице
    # несколько data-qa="submit-button" от разных шагов SPA).
    await password_input.press("Enter")
    await page.wait_for_load_state("domcontentloaded")
    await page.wait_for_timeout(2000)


async def try_login(chat_id: str | int, email: str, password: str) -> bool:
    """Тестовый логин с заданными кредами — для онбординга и смены пароля.

    Берёт контекст через browser_pool.acquire, выполняет _perform_login,
    при успехе сохраняет storage_state для chat_id.
    Возвращает True/False. Исключения браузера — подавляются и логируются.
    """
    try:
        async with browser_pool.acquire(chat_id) as context:
            page = await context.new_page()
            try:
                try:
                    await _perform_login(page, email, password)
                except Exception as e:
                    log.warning(f"auth._perform_login raised for chat_id={chat_id}: {e}")
                    await _dump_debug(page, chat_id, "perform_error")
                    return False
                if not await _is_authorised(page):
                    await _dump_debug(page, chat_id, "not_authorised")
                    return False
                await browser_pool.save_context(context, chat_id)
                return True
            finally:
                await page.close()
    except Exception as e:
        log.warning(f"auth.try_login failed for chat_id={chat_id}: {e}")
        return False


async def ensure_logged_in(context: BrowserContext, chat_id: str | int) -> None:
    """Гарантирует авторизованную сессию для chat_id.

    Пытается открыть главную. Если не авторизован — логинится и сохраняет storage_state.
    Поднимает LoginError при провале.
    """
    page = await context.new_page()
    try:
        await page.goto("https://rabota.by", wait_until="domcontentloaded", timeout=30000)
        if await _is_authorised(page):
            return

        log.info(f"auth: not logged in for chat_id={chat_id}, performing login")
        email, password = await _load_credentials(chat_id)
        await _perform_login(page, email, password)

        if not await _is_authorised(page):
            raise LoginError(f"login failed for chat_id={chat_id}")

        await browser_pool.save_context(context, chat_id)
        log.info(f"auth: logged in and session saved for chat_id={chat_id}")
    finally:
        await page.close()
