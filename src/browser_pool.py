"""Единый Playwright-браузер для всех модулей."""
import asyncio
import os
from contextlib import asynccontextmanager
from typing import Optional
from urllib.parse import urlparse

from playwright.async_api import async_playwright, Browser, BrowserContext, Playwright

from src.config import settings, log


def _parse_proxy(url: Optional[str]) -> Optional[dict]:
    if not url:
        return None
    p = urlparse(url)
    if not p.hostname:
        log.warning(f"Invalid PROXY_URL: {url}")
        return None
    scheme = p.scheme or "http"
    port = f":{p.port}" if p.port else ""
    proxy = {"server": f"{scheme}://{p.hostname}{port}"}
    if p.username:
        proxy["username"] = p.username
    if p.password:
        proxy["password"] = p.password
    return proxy

_browser: Optional[Browser] = None
_playwright: Optional[Playwright] = None
_semaphore: asyncio.Semaphore = asyncio.Semaphore(1)

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)


def _detect_chrome_channel() -> Optional[str]:
    if os.name == "nt":
        for path in [
            os.path.join(os.environ.get("PROGRAMFILES", ""), "Google", "Chrome", "Application", "chrome.exe"),
            os.path.join(os.environ.get("PROGRAMFILES(X86)", ""), "Google", "Chrome", "Application", "chrome.exe"),
            os.path.join(os.environ.get("LOCALAPPDATA", ""), "Google", "Chrome", "Application", "chrome.exe"),
        ]:
            if os.path.exists(path):
                return "chrome"
    return None


async def get_browser() -> Browser:
    global _browser, _playwright
    if _browser is None or not _browser.is_connected():
        if _playwright is None:
            _playwright = await async_playwright().start()
        channel = _detect_chrome_channel()
        launch_kwargs = dict(
            headless=True,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-dev-shm-usage",
            ],
        )
        if channel:
            launch_kwargs["channel"] = channel
            log.info(f"Browser pool: using system Chrome (channel={channel})")
        proxy = _parse_proxy(settings.proxy_url)
        if proxy:
            launch_kwargs["proxy"] = proxy
            log.info(f"Browser pool: using proxy {proxy['server']}")
        _browser = await _playwright.chromium.launch(**launch_kwargs)
    return _browser


def _session_path(chat_id: str | int) -> str:
    return os.path.join(settings.sessions_dir, str(chat_id), "storage_state.json")


async def get_context(chat_id: str | int) -> BrowserContext:
    """Контекст браузера с per-user storage_state.

    Загружает `sessions_dir/{chat_id}/storage_state.json` если файл есть.
    Сохранение делает `save_context()` или `auth.ensure_logged_in()`.
    """
    browser = await get_browser()
    path = _session_path(chat_id)
    storage_state = path if os.path.exists(path) else None
    context = await browser.new_context(
        storage_state=storage_state,
        user_agent=USER_AGENT,
        viewport={"width": 1920, "height": 1080},
    )
    return context


async def save_context(context: BrowserContext, chat_id: str | int) -> str:
    path = _session_path(chat_id)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    await context.storage_state(path=path)
    return path


@asynccontextmanager
async def acquire(chat_id: str | int, save_on_exit: bool = True):
    """Эксклюзивный доступ к браузеру: семафор + per-user контекст.

    На выходе сохраняет storage_state (если save_on_exit) и закрывает контекст,
    браузер живёт дальше. Семафор не даёт двум юзерам работать параллельно
    в одном инстансе браузера.
    """
    async with _semaphore:
        context = await get_context(chat_id)
        try:
            yield context
        finally:
            try:
                if save_on_exit:
                    await save_context(context, chat_id)
            except Exception:
                pass
            try:
                await context.close()
            except Exception:
                pass


def wipe_session(chat_id: str | int) -> None:
    """Удалить сохранённый storage_state для chat_id (re-registration)."""
    import shutil
    folder = os.path.join(settings.sessions_dir, str(chat_id))
    if os.path.isdir(folder):
        shutil.rmtree(folder, ignore_errors=True)


async def restart():
    """Принудительно пересоздать браузер (при сетевых ошибках)."""
    global _browser, _playwright
    log.warning("Browser pool: restarting browser")
    try:
        if _browser and _browser.is_connected():
            await _browser.close()
    except Exception:
        pass
    _browser = None
    if _playwright is not None:
        try:
            await _playwright.stop()
        except Exception:
            pass
        _playwright = None
    await get_browser()
    log.info("Browser pool: browser restarted successfully")


NETWORK_ERRORS = (
    "ERR_CONNECTION_CLOSED",
    "ERR_CONNECTION_RESET",
    "ERR_CONNECTION_REFUSED",
    "ERR_CONNECTION_TIMED_OUT",
    "ERR_NETWORK_CHANGED",
    "ERR_INTERNET_DISCONNECTED",
    "ERR_PROXY_CONNECTION_FAILED",
    "ERR_TUNNEL_CONNECTION_FAILED",
    "ERR_TIMED_OUT",
    "ERR_EMPTY_RESPONSE",
)


def is_network_error(exc: Exception) -> bool:
    msg = str(exc)
    return any(code in msg for code in NETWORK_ERRORS)


async def close():
    global _browser, _playwright
    if _browser and _browser.is_connected():
        await _browser.close()
    _browser = None
    if _playwright is not None:
        await _playwright.stop()
        _playwright = None
