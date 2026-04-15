import hashlib
from typing import List

from src.config import log
from src.models import Message
from src import browser_pool

INBOX_SELECTORS = {
    "responses_list": "div.responses-list, div[data-qa='responses-list']",
    "response_item": "div.response-item, div[data-qa='response-item']",
    "unread_badge": ".unread, .badge, [data-qa='unread-indicator']",
    "company_name": "span.company-name, a[data-qa='response-company']",
    "vacancy_title": "span.vacancy-title, a[data-qa='response-vacancy']",
    "message_item": "div.message-item, div[data-qa='message-item']",
    "message_text": "div.message-text, div[data-qa='message-text']",
    "message_sender": "span.message-sender, span[data-qa='message-sender']",
    "message_time": "span.message-time, time[data-qa='message-time']",
}



def _generate_message_id(conv_id: str, text: str, sender: str = "") -> str:
    raw = f"{conv_id}:{sender}:{text[:100]}"
    return hashlib.md5(raw.encode()).hexdigest()


async def check_inbox(chat_id: str | int) -> List[Message]:
    new_messages: List[Message] = []

    async with browser_pool.acquire(chat_id) as context:
        page = await context.new_page()
        try:
            # Перейти на страницу откликов
            await page.goto(
                "https://rabota.by/applicant/responses",
                wait_until="domcontentloaded",
                timeout=30000,
            )

            # Проверить авторизацию
            auth_el = await page.query_selector(
                "[data-qa='mainmenu_myResumes'], .applicant-sidebar"
            )
            if not auth_el:
                log.warning("Inbox: not authenticated, trying to re-login")
                from src.auth import ensure_logged_in
                await ensure_logged_in(context, chat_id)
                await page.goto(
                    "https://rabota.by/applicant/responses",
                    wait_until="domcontentloaded",
                )

            # Найти переписки
            items = await page.query_selector_all(INBOX_SELECTORS["response_item"])
            if not items:
                # Fallback: попробовать найти любые элементы списка
                items = await page.query_selector_all(
                    "div[class*='response'], div[class*='negotiation'], li[class*='item']"
                )

            log.info(f"Inbox: found {len(items)} response items")

            for item in items:
                try:
                    # Проверить маркер непрочитанного
                    unread = await item.query_selector(INBOX_SELECTORS["unread_badge"])
                    if not unread:
                        # Проверить по классу
                        class_attr = await item.get_attribute("class") or ""
                        if "unread" not in class_attr.lower() and "new" not in class_attr.lower():
                            continue

                    # Извлечь данные
                    company_el = await item.query_selector(INBOX_SELECTORS["company_name"])
                    company = (await company_el.inner_text()).strip() if company_el else None

                    title_el = await item.query_selector(INBOX_SELECTORS["vacancy_title"])
                    vacancy_title = (await title_el.inner_text()).strip() if title_el else None

                    # Получить ссылку на переписку
                    link = await item.query_selector("a[href]")
                    href = await link.get_attribute("href") if link else None
                    conv_id = href.split("/")[-1] if href else None

                    if not conv_id:
                        continue

                    # Открыть переписку
                    if link:
                        await link.click()
                        await page.wait_for_load_state("domcontentloaded")
                        await page.wait_for_timeout(1500)

                    # Собрать сообщения
                    msg_items = await page.query_selector_all(INBOX_SELECTORS["message_item"])
                    if not msg_items:
                        msg_items = await page.query_selector_all(
                            "div[class*='message'], div[class*='chat-message']"
                        )

                    for msg_el in msg_items:
                        text_el = await msg_el.query_selector(INBOX_SELECTORS["message_text"])
                        if not text_el:
                            text_el = msg_el
                        text = (await text_el.inner_text()).strip()
                        if not text:
                            continue

                        sender_el = await msg_el.query_selector(INBOX_SELECTORS["message_sender"])
                        sender = (await sender_el.inner_text()).strip() if sender_el else None

                        msg_id = _generate_message_id(conv_id, text, sender or "")

                        new_messages.append(Message(
                            message_id=msg_id,
                            text=text,
                            direction="incoming",
                            sender=sender,
                            vacancy_title=vacancy_title,
                            company=company,
                            conversation_id=conv_id,
                        ))

                    # Вернуться к списку
                    await page.go_back()
                    await page.wait_for_load_state("domcontentloaded")
                    await page.wait_for_timeout(1000)

                except Exception as e:
                    log.warning(f"Inbox: error processing item: {e}")
                    continue

            log.info(f"Inbox: {len(new_messages)} new messages found")

        except Exception as e:
            log.error(f"Inbox check error: {e}")

    return new_messages
