from typing import Tuple

from src.config import log
from src import browser_pool


async def send_reply(conversation_id: str, text: str, chat_id: str | int) -> Tuple[bool, str]:
    async with browser_pool.acquire(chat_id) as context:
        page = await context.new_page()

        try:
            await page.goto(
                f"https://rabota.by/applicant/responses/{conversation_id}",
                wait_until="networkidle",
                timeout=30000,
            )

            # Проверить авторизацию
            auth_el = await page.query_selector(
                "[data-qa='mainmenu_myResumes'], .applicant-sidebar"
            )
            if not auth_el:
                from src.auth import ensure_logged_in
                await ensure_logged_in(context, chat_id)
                await page.goto(
                    f"https://rabota.by/applicant/responses/{conversation_id}",
                    wait_until="networkidle",
                )

            # Найти поле ввода ответа
            textarea = await page.query_selector(
                "textarea[name='text'], textarea[data-qa='message-input']"
                ", div[contenteditable='true']"
                ", textarea"
            )

            if not textarea:
                return False, "Поле ввода не найдено"

            tag = await textarea.evaluate("el => el.tagName.toLowerCase()")
            if tag == "div":
                # contenteditable div
                await textarea.click()
                await textarea.type(text, delay=30)
            else:
                await textarea.fill(text)

            await page.wait_for_timeout(500)

            # Нажать «Отправить»
            send_btn = await page.query_selector(
                "button[data-qa='message-submit'], button:has-text('Отправить')"
                ", button[type='submit']"
            )

            if not send_btn:
                return False, "Кнопка отправки не найдена"

            await send_btn.click()
            await page.wait_for_timeout(3000)

            # Проверить что сообщение появилось
            page_text = await page.inner_text("body")
            if text[:50] in page_text:
                log.info(f"Reply sent to conversation {conversation_id}")
                return True, "OK"

            return True, "OK (unconfirmed)"

        except Exception as e:
            log.error(f"Reply error for {conversation_id}: {e}")
            return False, str(e)
