import re
import warnings

from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup, BotCommand,
)
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    ConversationHandler, MessageHandler, ContextTypes, filters,
)

from src.config import settings, log
from src import database, ai_filter, cover_flow

# ConversationHandler states
WAITING_REPLY = 1
WAITING_COVER_TEXT = 2
WAITING_SETTING_VALUE = 3

_app: Application = None


def _escape_md(text: str) -> str:
    if not text:
        return ""
    return re.sub(r'([_*\[\]()~`>#+\-=|{}.!\\])', r'\\\1', str(text))


def _vacancy_card_text(v, today_left: int = None) -> str:
    title = _escape_md(v.title)
    company = _escape_md(v.company or "Не указана")
    salary = _escape_md(v.salary or "Не указана")
    city = _escape_md(v.city or "Не указан")
    score = v.relevance_score
    reason = _escape_md(v.relevance_reason or "")

    # Индикатор: требуется ли сопроводительное
    if v.require_cover_letter:
        cover = "Требуется сопроводительное письмо"
    elif v.cover_letter:
        cover = "Сопроводительное сгенерировано"
    else:
        cover = "Без сопроводительного"

    lines = [
        f"*{title}*",
        f"{'─' * 20}",
        f"Компания: *{company}*",
        f"Зарплата: {salary}",
        f"Город: {city}",
        "",
        f"Релевантность: *{score}/100*",
        f"_{reason}_",
        "",
        f"_{_escape_md(cover)}_",
    ]

    if today_left is not None:
        lines.append(f"\nОткликов осталось: *{today_left}*")

    return "\n".join(lines)


def _vacancy_keyboard(vacancy_id: int, url: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("Откликнуться", callback_data=f"apply:{vacancy_id}"),
            InlineKeyboardButton("Пропустить", callback_data=f"skip:{vacancy_id}"),
        ],
        [
            InlineKeyboardButton("Открыть на сайте", url=url),
        ],
    ])


def _message_card_text(msg) -> str:
    company = _escape_md(msg.company or "Не указана")
    vacancy = _escape_md(msg.vacancy_title or "Не указана")
    sender = _escape_md(msg.sender or "Работодатель")
    text = _escape_md(msg.text or "")

    return (
        f"*Новое сообщение от работодателя*\n"
        f"{'─' * 20}\n"
        f"Компания: *{company}*\n"
        f"Вакансия: {vacancy}\n\n"
        f"*{sender}:*\n"
        f"{text}"
    )


def _message_keyboard(conversation_id: str, url: str = None) -> InlineKeyboardMarkup:
    buttons = [
        [
            InlineKeyboardButton("Ответить", callback_data=f"reply:{conversation_id}"),
            InlineKeyboardButton("AI-ответ", callback_data=f"ai_reply:{conversation_id}"),
        ],
    ]
    if url:
        buttons.append([InlineKeyboardButton("Открыть", url=url)])
    return InlineKeyboardMarkup(buttons)


# ─── Send functions ──────────────────────────────

async def send_vacancy_card(chat_id: str, vacancy):
    if not _app:
        return
    today_count = await database.count_today_applies()
    max_applies = await database.get_setting_int(chat_id, "max_applies_per_day", 10)
    today_left = max_applies - today_count
    text = _vacancy_card_text(vacancy, today_left=today_left)
    keyboard = _vacancy_keyboard(vacancy.id, vacancy.url)
    await _app.bot.send_message(
        chat_id=chat_id,
        text=text,
        reply_markup=keyboard,
        parse_mode="MarkdownV2",
    )


async def send_message_card(chat_id: str, msg, vacancy=None):
    if not _app:
        return
    text = _message_card_text(msg)
    conv_id = msg.conversation_id or msg.message_id
    url = f"https://rabota.by/applicant/responses/{conv_id}" if conv_id else None
    keyboard = _message_keyboard(conv_id, url)
    await _app.bot.send_message(
        chat_id=chat_id,
        text=text,
        reply_markup=keyboard,
        parse_mode="MarkdownV2",
    )


async def send_text(chat_id: str, text: str):
    if not _app:
        return
    await _app.bot.send_message(
        chat_id=chat_id,
        text=text,
    )


# ─── Command handlers ───────────────────────────

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Rabota Hunter Bot\n\n"
        "Команды:\n"
        "/stats - статистика\n"
        "/search - запустить парсинг сейчас\n"
        "/last - последние 5 вакансий\n"
        "/inbox - непрочитанные сообщения\n"
        "/threads - активные переписки\n"
        "/settings - настройки поиска и профиль"
    )


async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    stats = await database.get_stats()
    await update.message.reply_text(
        f"Статистика:\n"
        f"Всего вакансий: {stats['total']}\n"
        f"Отправлено в TG: {stats['sent_to_tg']}\n"
        f"Откликов: {stats['applied']}\n"
        f"Откликов сегодня: {stats['today_applied']}"
    )


async def cmd_last(update: Update, context: ContextTypes.DEFAULT_TYPE):
    vacancies = await database.get_last_vacancies(5)
    if not vacancies:
        await update.message.reply_text("Вакансий пока нет.")
        return
    for v in vacancies:
        text = (
            f"{v.title}\n"
            f"Компания: {v.company or 'N/A'}\n"
            f"Релевантность: {v.relevance_score}/100\n"
            f"Статус: {v.status}\n"
            f"{v.url}"
        )
        await update.message.reply_text(text)


async def cmd_search(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = str(update.effective_chat.id)
    from src import pipeline
    await pipeline.run_pipeline_for_user(chat_id)


async def cmd_inbox(update: Update, context: ContextTypes.DEFAULT_TYPE):
    messages = await database.get_unread_messages()
    if not messages:
        await update.message.reply_text("Нет непрочитанных сообщений.")
        return
    for msg in messages[:10]:
        text = (
            f"От: {msg.sender or msg.company or 'Работодатель'}\n"
            f"Вакансия: {msg.vacancy_title or 'N/A'}\n"
            f"{msg.text[:300]}"
        )
        await update.message.reply_text(text)


async def cmd_threads(update: Update, context: ContextTypes.DEFAULT_TYPE):
    convs = await database.get_active_conversations()
    if not convs:
        await update.message.reply_text("Нет активных переписок.")
        return
    lines = []
    for c in convs[:10]:
        status_icon = {"active": ">>", "waiting_reply": "??", "replied": "ok"}.get(c.status, "")
        lines.append(f"[{status_icon}] {c.company or 'N/A'} - {c.vacancy_title or 'N/A'}")
    await update.message.reply_text("Активные переписки:\n\n" + "\n".join(lines))


# ─── Settings ──────────────────────────────────

# Настройки, доступные для редактирования через бота
EDITABLE_SETTINGS = {
    "min_relevance_score": "Мин. релевантность (0-100)",
    "max_pages": "Макс. страниц поиска",
    "search_city": "Город поиска",
    "search_queries": "Ключевые слова (через запятую)",
    "scrape_interval_minutes": "Интервал поиска (мин)",
    "message_check_interval_minutes": "Проверка сообщений (мин)",
    "max_applies_per_day": "Макс. откликов в день",
}


async def cmd_settings(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = str(update.effective_chat.id)
    all_settings = await database.get_all_settings(chat_id)

    lines = ["*Настройки*\n"]
    for key, label in EDITABLE_SETTINGS.items():
        val = _escape_md(all_settings.get(key, "—"))
        lines.append(f"{_escape_md(label)}: *{val}*")

    # Профиль — показываем только имя
    name = all_settings.get("candidate_name", "Не задано")
    lines.append(f"\nПрофиль: *{_escape_md(name)}*")

    buttons = []
    keys = list(EDITABLE_SETTINGS.keys())
    for i in range(0, len(keys), 2):
        row = []
        for k in keys[i:i+2]:
            row.append(InlineKeyboardButton(
                EDITABLE_SETTINGS[k].split("(")[0].strip(),
                callback_data=f"set:{k}",
            ))
        buttons.append(row)
    buttons.append([
        InlineKeyboardButton("Профиль", callback_data="set:candidate_profile"),
    ])

    await update.message.reply_text(
        "\n".join(lines),
        parse_mode="MarkdownV2",
        reply_markup=InlineKeyboardMarkup(buttons),
    )


async def callback_set_setting(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    key = query.data.split(":", 1)[1]
    chat_id = str(query.message.chat_id)

    current = await database.get_setting(chat_id, key, "—")
    label = EDITABLE_SETTINGS.get(key, key)

    if key == "candidate_profile":
        label = "Профиль кандидата"
        await query.message.reply_text(
            f"Текущий профиль:\n\n{current}\n\nОтправь новый текст профиля (или /cancel):"
        )
    else:
        await query.message.reply_text(
            f"{label}\nТекущее значение: {current}\n\nОтправь новое значение (или /cancel):"
        )

    context.user_data["editing_setting"] = key
    return WAITING_SETTING_VALUE


async def receive_setting_value(update: Update, context: ContextTypes.DEFAULT_TYPE):
    key = context.user_data.pop("editing_setting", None)
    if not key:
        await update.message.reply_text("Нет активной настройки для редактирования.")
        return ConversationHandler.END

    chat_id = str(update.effective_chat.id)
    value = update.message.text.strip()

    # Валидация числовых настроек
    int_keys = {"min_relevance_score", "max_pages", "scrape_interval_minutes",
                "message_check_interval_minutes", "max_applies_per_day"}
    if key in int_keys:
        try:
            int(value)
        except ValueError:
            await update.message.reply_text("Нужно число. Попробуй ещё раз через /settings.")
            return ConversationHandler.END

    await database.set_setting(chat_id, key, value)
    label = EDITABLE_SETTINGS.get(key, key)
    await update.message.reply_text(f"Сохранено: {label} = {value}")
    return ConversationHandler.END


async def cancel_setting_edit(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.pop("editing_setting", None)
    await update.message.reply_text("Отменено.")
    return ConversationHandler.END


# ─── Callback handlers ──────────────────────────

def _cover_preview_keyboard(vacancy_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("Отправить", callback_data=f"cover_send:{vacancy_id}"),
            InlineKeyboardButton("Изменить", callback_data=f"cover_edit:{vacancy_id}"),
        ],
        [
            InlineKeyboardButton("Новый вариант", callback_data=f"cover_regen:{vacancy_id}"),
            InlineKeyboardButton("Отмена", callback_data=f"cover_cancel:{vacancy_id}"),
        ],
    ])


def _cover_user_text_keyboard(vacancy_id: int) -> InlineKeyboardMarkup:
    """Кнопки после того как пользователь прислал свой текст письма."""
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("Отправить", callback_data=f"cover_send:{vacancy_id}"),
            InlineKeyboardButton("AI-доработка", callback_data=f"cover_aifix:{vacancy_id}"),
        ],
        [
            InlineKeyboardButton("Отмена", callback_data=f"cover_cancel:{vacancy_id}"),
        ],
    ])


async def callback_apply(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Кнопка 'Откликнуться' — запуск cover letter flow."""
    query = update.callback_query
    await query.answer()
    vacancy_id = int(query.data.split(":")[1])

    chat_id = str(query.message.chat_id)
    max_applies = await database.get_setting_int(chat_id, "max_applies_per_day", 10)
    today_count = await database.count_today_applies()
    if today_count >= max_applies:
        await query.message.reply_text(
            f"Лимит откликов на сегодня ({max_applies}) исчерпан."
        )
        return

    # Контроль: один контекст за раз
    if context.user_data.get("editing_vacancy"):
        await query.message.reply_text(
            "Уже идёт работа над другим письмом. Завершите или отмените."
        )
        return

    await query.message.reply_text("Генерирую сопроводительное письмо...")

    try:
        result = await cover_flow.start_cover_letter(vacancy_id, chat_id=chat_id)
    except ValueError as e:
        await query.message.reply_text(f"Ошибка: {e}")
        return

    context.user_data["editing_vacancy"] = vacancy_id

    preview = cover_flow.format_preview(
        result["cover_letter"], result["requirements"], result["version"]
    )
    keyboard = _cover_preview_keyboard(vacancy_id)

    today_left = max_applies - today_count
    footer = f"\n\n📊 Осталось откликов сегодня: {today_left}"

    await query.message.reply_text(
        preview + footer,
        reply_markup=keyboard,
        parse_mode="HTML",
    )


async def callback_skip(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    vacancy_id = int(query.data.split(":")[1])
    await database.update_status(vacancy_id, "skipped")
    await query.edit_message_reply_markup(reply_markup=None)
    await query.message.reply_text("Пропущено.")


# ─── Cover letter flow ─────────────────────────

async def callback_cover_send(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Отправить отклик с текущим письмом."""
    query = update.callback_query
    await query.answer()
    vacancy_id = int(query.data.split(":")[1])

    vacancy = await database.get_vacancy(vacancy_id)
    if not vacancy or not vacancy.cover_letter:
        await query.message.reply_text("Нет письма для отправки.")
        context.user_data.pop("editing_vacancy", None)
        return

    try:
        await cover_flow.confirm_send(vacancy_id)
    except ValueError as e:
        await query.message.reply_text(f"Ошибка: {e}")
        return

    await query.edit_message_reply_markup(reply_markup=None)
    await query.message.reply_text("Отправляю отклик...")

    from src import applier
    chat_id = str(update.effective_chat.id)
    success, message = await applier.apply_to_vacancy(vacancy, chat_id)

    if success:
        await cover_flow.mark_sent(vacancy_id)
        await database.update_status(vacancy_id, "applied")
        await query.message.reply_text(f"Отклик отправлен: {vacancy.title}")
    else:
        await cover_flow.mark_failed(vacancy_id)
        await database.update_status(vacancy_id, "error", message)
        await query.message.reply_text(f"Ошибка отклика: {message}")

    context.user_data.pop("editing_vacancy", None)


async def callback_cover_edit(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Перейти в режим редактирования — пользователь пишет своё письмо."""
    query = update.callback_query
    await query.answer()
    vacancy_id = int(query.data.split(":")[1])

    try:
        await cover_flow.enter_editing(vacancy_id)
    except ValueError as e:
        await query.message.reply_text(f"Ошибка: {e}")
        return

    context.user_data["editing_vacancy"] = vacancy_id
    await query.edit_message_reply_markup(reply_markup=None)
    await query.message.reply_text(
        "Напиши своё сопроводительное письмо:\n(/cancel для отмены)"
    )
    return WAITING_COVER_TEXT


async def receive_cover_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Получен текст письма от пользователя."""
    text = update.message.text
    vacancy_id = context.user_data.get("editing_vacancy")
    if not vacancy_id:
        await update.message.reply_text("Нет активного контекста редактирования.")
        return ConversationHandler.END

    try:
        result = await cover_flow.submit_user_text(vacancy_id, text)
    except ValueError as e:
        await update.message.reply_text(f"Ошибка: {e}")
        return ConversationHandler.END

    keyboard = _cover_user_text_keyboard(vacancy_id)
    await update.message.reply_text(
        f"Твоё письмо:\n\n{text}\n\nОтправить или доработать AI?",
        reply_markup=keyboard,
    )
    return ConversationHandler.END


async def cancel_cover_edit(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Отмена редактирования письма по /cancel."""
    vacancy_id = context.user_data.pop("editing_vacancy", None)
    if vacancy_id:
        await cover_flow.cancel(vacancy_id)
    await update.message.reply_text("Редактирование отменено.")
    return ConversationHandler.END


async def callback_cover_aifix(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """AI-доработка текста пользователя с учётом требований."""
    query = update.callback_query
    await query.answer("Дорабатываю текст...")
    vacancy_id = int(query.data.split(":")[1])

    try:
        result = await cover_flow.ai_improve_user_text(vacancy_id)
    except ValueError as e:
        await query.message.reply_text(f"Ошибка: {e}")
        return

    preview = f"✉️ <b>Доработанное письмо:</b>\n{result['cover_letter']}"
    keyboard = _cover_preview_keyboard(vacancy_id)

    await query.message.reply_text(preview, reply_markup=keyboard, parse_mode="HTML")


async def callback_cover_regen(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Сгенерировать новый вариант письма."""
    query = update.callback_query
    await query.answer("Генерирую новый вариант...")
    vacancy_id = int(query.data.split(":")[1])

    try:
        chat_id = str(query.message.chat_id)
        result = await cover_flow.regenerate_cover_letter(vacancy_id, chat_id=chat_id)
    except ValueError as e:
        await query.message.reply_text(f"Ошибка: {e}")
        return

    preview = cover_flow.format_preview(
        result["cover_letter"], result["requirements"], result["version"]
    )
    keyboard = _cover_preview_keyboard(vacancy_id)

    await query.message.reply_text(preview, reply_markup=keyboard, parse_mode="HTML")


async def callback_cover_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Отмена cover letter flow."""
    query = update.callback_query
    await query.answer()
    vacancy_id = int(query.data.split(":")[1])

    await cover_flow.cancel(vacancy_id)
    context.user_data.pop("editing_vacancy", None)

    await query.edit_message_reply_markup(reply_markup=None)
    await query.message.reply_text("Отклик отменён.")


# ─── Reply conversation ─────────────────────────

async def callback_reply(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    conversation_id = query.data.split(":")[1]
    context.user_data["reply_to"] = conversation_id
    await query.message.reply_text(
        "Напиши ответ работодателю:\n(/cancel для отмены)"
    )
    return WAITING_REPLY


async def receive_reply_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    conversation_id = context.user_data.get("reply_to")
    if not conversation_id:
        await update.message.reply_text("Нет активной переписки.")
        return ConversationHandler.END

    context.user_data["reply_text"] = text

    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("Отправить", callback_data=f"send:{conversation_id}"),
            InlineKeyboardButton("Улучшить", callback_data=f"improve:{conversation_id}"),
        ],
        [
            InlineKeyboardButton("Отмена", callback_data=f"cancel_reply:{conversation_id}"),
        ],
    ])

    await update.message.reply_text(
        f"Твой ответ:\n\n{text}\n\nОтправить?",
        reply_markup=keyboard,
    )
    return ConversationHandler.END


async def cancel_reply(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.pop("reply_to", None)
    context.user_data.pop("reply_text", None)
    await update.message.reply_text("Отменено.")
    return ConversationHandler.END


async def callback_ai_reply(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer("Генерирую ответ...")
    conversation_id = query.data.split(":")[1]

    history = await database.get_conversation_history(conversation_id)
    vacancy = await database.get_vacancy_by_conversation(conversation_id)

    chat_id = str(query.message.chat_id)
    ai_text = await ai_filter.generate_reply(vacancy, history, chat_id)
    if not ai_text:
        await query.message.reply_text("Не удалось сгенерировать ответ.")
        return

    context.user_data["reply_to"] = conversation_id
    context.user_data["reply_text"] = ai_text

    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("Отправить", callback_data=f"send:{conversation_id}"),
            InlineKeyboardButton("Изменить", callback_data=f"reply:{conversation_id}"),
        ],
        [
            InlineKeyboardButton("Другой вариант", callback_data=f"ai_reply:{conversation_id}"),
        ],
    ])

    await query.message.reply_text(
        f"AI-ответ:\n\n{ai_text}\n\nОтправить?",
        reply_markup=keyboard,
    )


async def callback_send(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    conversation_id = query.data.split(":")[1]
    text = context.user_data.get("reply_text")

    if not text:
        await query.message.reply_text("Нет текста для отправки.")
        return

    from src import responder
    chat_id = str(update.effective_chat.id)
    success, message = await responder.send_reply(conversation_id, text, chat_id)

    if success:
        await database.save_outgoing_message(conversation_id, text)
        await query.message.reply_text("Ответ отправлен!")
        await query.edit_message_reply_markup(reply_markup=None)
    else:
        await query.message.reply_text(f"Ошибка: {message}")

    context.user_data.pop("reply_to", None)
    context.user_data.pop("reply_text", None)


async def callback_improve(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer("Улучшаю текст...")
    conversation_id = query.data.split(":")[1]
    text = context.user_data.get("reply_text", "")

    improved = await ai_filter.improve_text(text)
    context.user_data["reply_text"] = improved

    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("Отправить", callback_data=f"send:{conversation_id}"),
            InlineKeyboardButton("Изменить", callback_data=f"reply:{conversation_id}"),
        ],
    ])

    await query.message.reply_text(
        f"Улучшенный текст:\n\n{improved}\n\nОтправить?",
        reply_markup=keyboard,
    )


async def callback_cancel_reply(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    context.user_data.pop("reply_to", None)
    context.user_data.pop("reply_text", None)
    await query.edit_message_reply_markup(reply_markup=None)
    await query.message.reply_text("Отменено.")


# ─── App builder ─────────────────────────────────

def create_app() -> Application:
    global _app
    _app = Application.builder().token(settings.telegram_bot_token).build()

    # Reply conversation handler
    # per_message=False т.к. entry point — CallbackQuery, а ответ — Message
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", message=".*per_message.*", category=UserWarning)
        reply_conv = ConversationHandler(
            entry_points=[
                CallbackQueryHandler(callback_reply, pattern=r"^reply:"),
            ],
            states={
                WAITING_REPLY: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, receive_reply_text),
                ],
            },
            fallbacks=[
                CommandHandler("cancel", cancel_reply),
            ],
            per_message=False,
            per_chat=True,
        )

    _app.add_handler(reply_conv)

    # Cover letter edit conversation handler
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", message=".*per_message.*", category=UserWarning)
        cover_edit_conv = ConversationHandler(
            entry_points=[
                CallbackQueryHandler(callback_cover_edit, pattern=r"^cover_edit:"),
            ],
            states={
                WAITING_COVER_TEXT: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, receive_cover_text),
                ],
            },
            fallbacks=[
                CommandHandler("cancel", cancel_cover_edit),
            ],
            per_message=False,
            per_chat=True,
        )

    _app.add_handler(cover_edit_conv)

    # Settings conversation handler
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", message=".*per_message.*", category=UserWarning)
        settings_conv = ConversationHandler(
            entry_points=[
                CallbackQueryHandler(callback_set_setting, pattern=r"^set:"),
            ],
            states={
                WAITING_SETTING_VALUE: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, receive_setting_value),
                ],
            },
            fallbacks=[
                CommandHandler("cancel", cancel_setting_edit),
            ],
            per_message=False,
            per_chat=True,
        )

    _app.add_handler(settings_conv)

    # Commands — группа -1 чтобы обрабатывались до ConversationHandler
    _app.add_handler(CommandHandler("start", cmd_start), group=-1)
    _app.add_handler(CommandHandler("stats", cmd_stats), group=-1)
    _app.add_handler(CommandHandler("last", cmd_last), group=-1)
    _app.add_handler(CommandHandler("search", cmd_search), group=-1)
    _app.add_handler(CommandHandler("inbox", cmd_inbox), group=-1)
    _app.add_handler(CommandHandler("threads", cmd_threads), group=-1)
    _app.add_handler(CommandHandler("settings", cmd_settings), group=-1)

    # Callbacks
    _app.add_handler(CallbackQueryHandler(callback_apply, pattern=r"^apply:"))
    _app.add_handler(CallbackQueryHandler(callback_skip, pattern=r"^skip:"))
    _app.add_handler(CallbackQueryHandler(callback_ai_reply, pattern=r"^ai_reply:"))
    _app.add_handler(CallbackQueryHandler(callback_send, pattern=r"^send:"))
    _app.add_handler(CallbackQueryHandler(callback_improve, pattern=r"^improve:"))
    _app.add_handler(CallbackQueryHandler(callback_cancel_reply, pattern=r"^cancel_reply:"))

    # Cover letter flow callbacks
    _app.add_handler(CallbackQueryHandler(callback_cover_send, pattern=r"^cover_send:"))
    _app.add_handler(CallbackQueryHandler(callback_cover_aifix, pattern=r"^cover_aifix:"))
    _app.add_handler(CallbackQueryHandler(callback_cover_regen, pattern=r"^cover_regen:"))
    _app.add_handler(CallbackQueryHandler(callback_cover_cancel, pattern=r"^cover_cancel:"))

    return _app
