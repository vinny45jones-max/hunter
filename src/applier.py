import os
from typing import Tuple

from src.config import log
from src.models import Vacancy
from src.scraper import SELECTORS
from src import browser_pool, auth


async def _check_auth(page) -> bool:
    try:
        el = await page.query_selector(
            "[data-qa='mainmenu_myResumes'], "
            ".applicant-sidebar, "
            "a[href*='applicant']"
        )
        return el is not None
    except Exception:
        return False


async def apply_to_vacancy(vacancy: Vacancy, chat_id: str | int) -> Tuple[bool, str]:
    async with browser_pool.acquire(chat_id) as context:
        page = await context.new_page()

        try:
            await page.goto(vacancy.url, wait_until="domcontentloaded", timeout=30000)

            # Проверить авторизацию
            if not await _check_auth(page):
                log.info(f"Apply [{chat_id}]: session expired, re-authenticating")
                await auth.ensure_logged_in(page.context, chat_id)
                await page.goto(vacancy.url, wait_until="domcontentloaded", timeout=30000)

            # Проверить что вакансия не закрыта
            page_text = await page.inner_text("body")
            if "вакансия в архиве" in page_text.lower() or "вакансия закрыта" in page_text.lower():
                return False, "Вакансия закрыта"

            if "вы уже откликнулись" in page_text.lower() or "отклик уже отправлен" in page_text.lower():
                return False, "Уже откликались"

            # Найти кнопку отклика
            apply_btn = await page.query_selector(SELECTORS["apply_button"])
            if not apply_btn:
                # Fallback: любая кнопка с текстом "Откликнуться"
                apply_btn = await page.query_selector("button:has-text('Откликнуться')")

            if not apply_btn:
                return False, "Кнопка отклика не найдена"

            await apply_btn.click()
            await page.wait_for_timeout(2000)

            # Проверить: появилось ли модальное окно с формой
            cover_textarea = await page.query_selector(
                "textarea[name='letter'], textarea[data-qa='vacancy-response-popup-form-letter-input']"
            )
            if cover_textarea and vacancy.cover_letter:
                await cover_textarea.fill(vacancy.cover_letter)

            # Нажать финальную кнопку отправки (в модалке)
            send_btn = await page.query_selector(
                "button[data-qa='vacancy-response-submit-popup'], "
                "button:has-text('Отправить'), "
                "button:has-text('Откликнуться')"
            )
            if send_btn:
                await send_btn.click()
                await page.wait_for_timeout(3000)

            # Проверить подтверждение
            confirmation = await page.query_selector(
                "[data-qa='vacancy-response-popup-sent'], "
                ":has-text('Отклик отправлен')"
            )

            # Скриншот для лога
            screenshots_dir = "/data/screenshots"
            os.makedirs(screenshots_dir, exist_ok=True)
            await page.screenshot(path=f"{screenshots_dir}/{vacancy.external_id}.png")

            if confirmation:
                log.info(f"Applied to: {vacancy.title}")
                return True, "OK"

            # Проверить капчу
            captcha = await page.query_selector(
                "iframe[src*='captcha'], div[class*='captcha'], .g-recaptcha"
            )
            if captcha:
                return False, f"Капча! Откликнись вручную: {vacancy.url}"

            # Если не уверены — считаем успехом (кнопка была нажата)
            return True, "OK (unconfirmed)"

        except Exception as e:
            log.error(f"Apply error for {vacancy.url}: {e}")

            # Скриншот ошибки
            try:
                screenshots_dir = "/data/screenshots"
                os.makedirs(screenshots_dir, exist_ok=True)
                await page.screenshot(path=f"{screenshots_dir}/{vacancy.external_id}_error.png")
            except Exception:
                pass

            return False, str(e)
