"""
Тест 5: Telegram bot — создание, хендлеры, escape, карточки.
Не требует реального токена — проверяет структуру, а не HTTP.
"""
import pytest
from src.bot import (
    _escape_md,
    _vacancy_card_text,
    _message_card_text,
    _vacancy_keyboard,
    _message_keyboard,
    create_app,
)
from src.models import Vacancy, Message


class TestEscapeMd:
    def test_basic_escape(self):
        assert _escape_md("hello_world") == r"hello\_world"

    def test_multiple_chars(self):
        result = _escape_md("a*b[c]d(e)f")
        assert r"\*" in result
        assert r"\[" in result
        assert r"\]" in result
        assert r"\(" in result
        assert r"\)" in result

    def test_empty(self):
        assert _escape_md("") == ""
        assert _escape_md(None) == ""

    def test_plain_text(self):
        assert _escape_md("Hello World") == "Hello World"

    def test_numbers(self):
        assert _escape_md("Score: 85/100") == "Score: 85/100"

    def test_all_special_chars(self):
        special = r"_*[]()~`>#+\-=|{}.!"
        result = _escape_md(special)
        # Каждый символ должен быть экранирован
        for ch in special:
            if ch == '\\':
                continue
            assert f"\\{ch}" in result or f"\\\\{ch}" in result


class TestVacancyCard:
    def test_card_text(self):
        v = Vacancy(
            external_id="1",
            url="https://r.by/1",
            title="CEO",
            company="Acme Corp",
            salary="5000 USD",
            city="Minsk",
            relevance_score=85,
            relevance_reason="Great match",
            cover_letter="Dear Sir...",
        )
        text = _vacancy_card_text(v)
        assert "CEO" in text
        assert "Acme Corp" in text
        assert "5000 USD" in text
        assert "85/100" in text
        assert "Great match" in text
        assert "составлю при отклике" in text

    def test_card_text_no_cover(self):
        v = Vacancy(
            external_id="2",
            url="u",
            title="CTO",
            relevance_score=60,
        )
        text = _vacancy_card_text(v)
        assert "Письмо под вакансию составлю при отклике" in text
        assert "Не указана" in text  # company/salary defaults

    def test_card_text_no_optionals(self):
        v = Vacancy(external_id="3", url="u", title="PM", relevance_score=0)
        text = _vacancy_card_text(v)
        assert "PM" in text
        assert "0/100" in text


class TestVacancyKeyboard:
    def test_keyboard_structure(self):
        kb = _vacancy_keyboard(42, "https://r.by/42")
        rows = kb.inline_keyboard
        assert len(rows) == 2

        # Row 1: Откликнуться + Пропустить
        assert len(rows[0]) == 2
        assert rows[0][0].callback_data == "apply:42"
        assert rows[0][1].callback_data == "skip:42"

        # Row 2: URL button
        assert len(rows[1]) == 1
        assert rows[1][0].url == "https://r.by/42"


class TestMessageCard:
    def test_message_card(self):
        msg = Message(
            message_id="m1",
            text="Приглашаем на собеседование",
            direction="incoming",
            sender="HR Manager",
            company="Acme Corp",
            vacancy_title="CEO",
        )
        text = _message_card_text(msg)
        assert "Новое сообщение" in text
        assert "Acme Corp" in text
        assert "CEO" in text
        assert "HR Manager" in text


class TestMessageKeyboard:
    def test_with_url(self):
        kb = _message_keyboard("conv1", "https://r.by/responses/conv1")
        rows = kb.inline_keyboard
        assert len(rows) == 2
        assert rows[0][0].callback_data == "reply:conv1"
        assert rows[0][1].callback_data == "ai_reply:conv1"
        assert rows[1][0].url == "https://r.by/responses/conv1"

    def test_without_url(self):
        kb = _message_keyboard("conv2")
        rows = kb.inline_keyboard
        assert len(rows) == 1  # No URL row


class TestCreateApp:
    def test_app_created(self):
        app = create_app()
        assert app is not None

    def test_handlers_registered(self):
        app = create_app()
        # ConversationHandler + 6 commands + 6 callbacks = 13 handlers
        handler_count = sum(len(g) for g in app.handlers.values())
        assert handler_count >= 13

    def test_handlers_include_commands(self):
        app = create_app()
        from telegram.ext import CommandHandler, ConversationHandler
        cmd_handlers = []
        for group in app.handlers.values():
            for h in group:
                if isinstance(h, CommandHandler):
                    cmd_handlers.append(h)
                elif isinstance(h, ConversationHandler):
                    for ep in h.entry_points:
                        if isinstance(ep, CommandHandler):
                            cmd_handlers.append(ep)
        commands = set()
        for h in cmd_handlers:
            commands.update(h.commands)
        assert "start" in commands
        assert "stats" in commands
        assert "search" in commands
        assert "last" in commands
        assert "inbox" in commands
        assert "threads" in commands

    def test_handlers_include_callbacks(self):
        app = create_app()
        from telegram.ext import CallbackQueryHandler
        cb_handlers = [
            h for group in app.handlers.values()
            for h in group
            if isinstance(h, CallbackQueryHandler)
        ]
        # apply, skip, ai_reply, send, improve, cancel_reply = 6
        assert len(cb_handlers) >= 6


class TestApplyOneClick:
    """«Откликнуться» без превью: письмо генерируется и отклик уходит сразу."""

    def _update(self, vacancy_id):
        from unittest.mock import AsyncMock, MagicMock
        update = MagicMock()
        update.callback_query.data = f"apply:{vacancy_id}"
        update.callback_query.answer = AsyncMock()
        update.callback_query.edit_message_reply_markup = AsyncMock()
        update.callback_query.message.chat_id = 42
        update.callback_query.message.reply_text = AsyncMock()
        context = MagicMock()
        context.user_data = {}
        return update, context

    async def _vacancy(self):
        from src import database
        return await database.save_vacancy(Vacancy(
            external_id="ext1", url="https://rabota.by/vacancy/1", title="Коммерческий директор",
            company="ООО Тест", description="Руководство продажами",
        ))

    @pytest.mark.asyncio
    async def test_generates_and_sends(self, init_db, monkeypatch):
        from unittest.mock import AsyncMock
        from src import bot, database, ai_filter, applier
        vid = await self._vacancy()
        monkeypatch.setattr(ai_filter, "generate_cover_letter", AsyncMock(return_value="Письмо под вакансию"))
        apply_mock = AsyncMock(return_value=(True, "OK"))
        monkeypatch.setattr(applier, "apply_to_vacancy", apply_mock)

        update, context = self._update(vid)
        await bot.callback_apply(update, context)

        sent_vacancy = apply_mock.await_args.args[0]
        assert sent_vacancy.cover_letter == "Письмо под вакансию"
        v = await database.get_vacancy(vid)
        assert v.status == "applied" and v.apply_state == "sent"
        update.callback_query.edit_message_reply_markup.assert_awaited_once()
        last = update.callback_query.message.reply_text.await_args.args[0]
        assert "Отклик отправлен" in last and "Письмо под вакансию" in last

    @pytest.mark.asyncio
    async def test_already_applied_not_resent(self, init_db, monkeypatch):
        from unittest.mock import AsyncMock
        from src import bot, database, applier
        vid = await self._vacancy()
        await database.update_status(vid, "applied")
        apply_mock = AsyncMock(return_value=(True, "OK"))
        monkeypatch.setattr(applier, "apply_to_vacancy", apply_mock)

        update, context = self._update(vid)
        await bot.callback_apply(update, context)

        apply_mock.assert_not_awaited()
