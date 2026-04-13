# План реализации: Cover Letter Flow (v3)

## 1. Расширение БД — новые поля в таблице vacancies
- [x] `apply_state TEXT DEFAULT 'idle'` — состояние отклика (idle | previewing | editing | sending | sent | failed)
- [x] `cover_letter_version INTEGER DEFAULT 0` — счётчик версий письма
- [x] `employer_requirements TEXT` — JSON: требования работодателя к письму
- [x] `require_cover_letter BOOLEAN DEFAULT 0` — обязательно ли письмо
- [x] `negotiation_id TEXT` — ID отклика на rabota.by
- [x] Миграция в `database.py` (ALTER TABLE или пересоздание)

## 2. extract_cover_letter_requirements()
- [ ] Функция в новом модуле `cover_flow.py`
- [ ] Паттерны поиска: "в сопроводительном письме", "в отклике укажите", "ответьте на вопрос", "в письме напишите", "обязательно укажите"
- [ ] Возвращает список требований (1-5 штук) или пустой список

## 3. Расширение ai_filter.py — новые промпты
- [ ] `generate_cover_letter()` — обновить промпт с учётом requirements и version_hint
- [ ] `improve_cover_letter()` — новая функция: доработка текста пользователя с учётом требований работодателя

## 4. cover_flow.py — логика состояний
- [ ] Управление apply_state в БД
- [ ] Формирование превью письма с требованиями работодателя
- [ ] Генерация нового варианта (другой ракурс, version_hint)

## 5. bot.py — новые callback handlers для cover letter flow
- [ ] `on_apply_click` — генерация письма + превью с кнопками [Отправить] [Изменить] [Новое] [Отмена]
- [ ] `on_send_click` — отправка отклика с письмом
- [ ] `on_edit_click` — переход в режим редактирования
- [ ] `on_user_text` — обработка текста пользователя (письмо)
- [ ] `on_aifix_click` — AI доработка текста пользователя
- [ ] `on_regen_click` — генерация нового варианта письма
- [ ] Контроль: один контекст за раз (editing_vacancy / replying_to)

## 6. Обновление карточки вакансии в Telegram
- [ ] Показ требований работодателя в превью письма
- [ ] Счётчик оставшихся откликов на сегодня

## 7. Тестирование
- [ ] Unit-тест extract_cover_letter_requirements()
- [ ] Unit-тест cover_flow state transitions
- [ ] Интеграционный тест: полный cover letter flow в TG
