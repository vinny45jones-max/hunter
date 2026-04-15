"""Per-user авторизация rabota.by.

Хранит storage_state в `sessions_dir/{chat_id}/storage_state.json`.
Креды достаются из `user_settings` (зашифрованный пароль через Fernet).
Если в БД пусто — fallback на глобальные `settings.rabota_email`/`settings.rabota_password`.
"""
from typing import Tuple

from playwright.async_api import BrowserContext, Page

from src.config import settings, log
from src import browser_pool, database


AUTH_MARKER_SELECTOR = (
    "[data-qa='mainmenu_myResumes'], "
    ".applicant-sidebar, "
    "a[href*='applicant']"
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
        el = await page.query_selector(AUTH_MARKER_SELECTOR)
        return el is not None
    except Exception:
        return False


async def _perform_login(page: Page, email: str, password: str) -> None:
    await page.goto(LOGIN_URL, wait_until="domcontentloaded", timeout=30000)

    # 1. "Я ищу работу"
    await page.click("text=Я ищу работу", timeout=10000)
    await page.wait_for_timeout(1000)

    # 2. "Войти"
    await page.click('button[data-qa="submit-button"]', timeout=5000)
    await page.wait_for_timeout(2000)

    # 3. "Почта"
    await page.click("text=Почта", timeout=5000)
    await page.wait_for_timeout(1000)

    # 4. Email
    email_input = await page.wait_for_selector(
        '[data-qa="applicant-login-input-email"]', timeout=5000,
    )
    await email_input.fill(email)

    # 5. "Войти с паролем"
    await page.click("text=Войти с паролем", timeout=5000)
    await page.wait_for_timeout(2000)

    # 6. Пароль
    password_input = await page.wait_for_selector(
        '[data-qa="applicant-login-input-password"]', timeout=5000,
    )
    await password_input.fill(password)

    # 7. Submit
    await page.click('button[data-qa="submit-button"]', timeout=5000)
    await page.wait_for_load_state("domcontentloaded")


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
                await _perform_login(page, email, password)
                if not await _is_authorised(page):
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
