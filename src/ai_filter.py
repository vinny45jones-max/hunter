import json
import re
from typing import List

import anthropic

from src.config import settings, log
from src.models import Vacancy, Message
from src import database

_client = anthropic.AsyncAnthropic(api_key=settings.anthropic_api_key, max_retries=5)
MODEL = "claude-sonnet-4-6"

SYSTEM_PROMPT = (
    "Ты -- рекрутер-аналитик. Оцениваешь релевантность вакансии для кандидата. "
    "Отвечай ТОЛЬКО валидным JSON без markdown-блоков."
)

BATCH_EVALUATE_PROMPT = """
ПРОФИЛЬ КАНДИДАТА — {candidate_name}:
{candidate_profile}

Оцени каждую вакансию по названию и компании (0-100).
Отвечай ТОЛЬКО JSON-массивом, без пояснений:
[{{"id": 0, "score": <int>}}, {{"id": 1, "score": <int>}}, ...]

ВАКАНСИИ:
{vacancies_block}
"""

EVALUATE_PROMPT = """
ПРОФИЛЬ КАНДИДАТА — {candidate_name}:
{candidate_profile}

ВАКАНСИЯ:
Название: {title}
Компания: {company}
Зарплата: {salary}
Город: {city}
Описание: {description}

ЗАДАЧИ:
1. Оцени вакансию: score (0-100) и reason (1-2 предложения)
2. Если score >= {min_score} — напиши сопроводительное письмо (3-5 предложений):
   - Тон: уверенный, конкретный, без воды. Русский язык.
   - Не начинай с "Уважаемый". Сразу к делу.
   - Покажи конкретную ценность для этой компании.
   Если score < {min_score} — cover_letter = null.
{requirements_block}
JSON: {{"score": <int>, "reason": "<str>", "cover_letter": "<str> или null"}}
"""

COVER_LETTER_PROMPT = """
Напиши сопроводительное письмо (3-5 предложений) для отклика на вакансию.

Вакансия: {title} в {company}
Описание: {description}
{requirements_block}
Кандидат — {candidate_name}:
{candidate_profile}
{version_block}
Тон: уверенный, конкретный, без воды. Русский язык.
Не начинай с "Уважаемый". Сразу к делу.
Покажи конкретную ценность для этой компании, привяжи к их задачам.
"""

IMPROVE_COVER_LETTER_PROMPT = """
Доработай сопроводительное письмо кандидата для отклика на вакансию.

Вакансия: {title} в {company}
Описание: {description}
{requirements_block}
Текст кандидата:
{user_text}

Задача:
- Сохрани основную мысль и стиль автора
- Сделай профессиональнее и конкретнее
- Убедись что письмо отвечает на требования работодателя (если есть)
- Убери воду, оставь суть
- Русский язык. Верни ТОЛЬКО текст письма.
"""

REPLY_PROMPT = """
Ты помогаешь соискателю ответить на сообщение работодателя.

ВАКАНСИЯ: {vacancy_title} в {company}
ОПИСАНИЕ ВАКАНСИИ: {vacancy_description}

ИСТОРИЯ ПЕРЕПИСКИ:
{conversation_history}

ПОСЛЕДНЕЕ СООБЩЕНИЕ ОТ РАБОТОДАТЕЛЯ:
{last_message}

ПРОФИЛЬ СОИСКАТЕЛЯ — {candidate_name}:
{candidate_profile}

ЗАДАЧА:
Напиши ответ (2-5 предложений).
- Если приглашают на собеседование — подтверди готовность, уточни формат
- Если задают вопрос — ответь по существу
- Если просят информацию — предоставь кратко
Тон: профессиональный, без подобострастия. Русский язык.
"""


async def get_candidate_info(chat_id: str) -> tuple[str, str]:
    """Возвращает (candidate_name, candidate_profile) из БД для конкретного юзера."""
    name = await database.get_setting(chat_id, "candidate_name", "Кандидат")
    profile = await database.get_setting(chat_id, "candidate_profile", "Профиль не заполнен")
    return name, profile


def _parse_json(text: str) -> dict:
    # Убрать markdown-обёртку если есть
    text = text.strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text)

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    # Fallback: regex
    score_match = re.search(r'"score"\s*:\s*(\d+)', text)
    reason_match = re.search(r'"reason"\s*:\s*"([^"]+)"', text)
    cover_match = re.search(r'"cover_letter"\s*:\s*"((?:[^"\\]|\\.)*)"', text)
    if score_match:
        result = {
            "score": int(score_match.group(1)),
            "reason": reason_match.group(1) if reason_match else "N/A",
        }
        if cover_match:
            result["cover_letter"] = cover_match.group(1).replace("\\n", "\n")
        return result
    raise ValueError(f"Cannot parse JSON from: {text[:200]}")


async def batch_evaluate_titles(vacancies: List[Vacancy], chat_id: str, batch_size: int = 30) -> dict[int, int]:
    """Быстрая оценка по названию+компания батчами. Возвращает {index: score}."""
    all_scores = {}
    candidate_name, candidate_profile = await get_candidate_info(chat_id)

    for start in range(0, len(vacancies), batch_size):
        batch = vacancies[start:start + batch_size]
        lines = []
        for i, v in enumerate(batch):
            idx = start + i
            lines.append(f"{idx}. {v.title} | {v.company or 'N/A'} | {v.salary or 'N/A'}")

        prompt = BATCH_EVALUATE_PROMPT.format(
            vacancies_block="\n".join(lines),
            candidate_name=candidate_name,
            candidate_profile=candidate_profile,
        )

        try:
            response = await _client.messages.create(
                model=MODEL,
                max_tokens=2000,
                system=SYSTEM_PROMPT,
                messages=[{"role": "user", "content": prompt}],
            )
            text = response.content[0].text.strip()
            # Убрать markdown
            if text.startswith("```"):
                text = re.sub(r"^```(?:json)?\s*", "", text)
                text = re.sub(r"\s*```$", "", text)

            results = json.loads(text)
            for item in results:
                all_scores[int(item["id"])] = max(0, min(100, int(item["score"])))

            log.info(f"Batch {start}-{start+len(batch)}: оценено {len(results)} вакансий")
        except Exception as e:
            log.warning(f"Batch evaluate error: {e}")
            # При ошибке даём средний балл чтобы не потерять вакансии
            for i in range(start, start + len(batch)):
                all_scores[i] = 40

    return all_scores


async def evaluate_and_cover(vacancy: Vacancy, chat_id: str, min_score: int = 60) -> dict:
    """Оценка + cover letter в одном вызове. Возвращает {score, reason, cover_letter}."""
    candidate_name, candidate_profile = await get_candidate_info(chat_id)

    req_block = ""
    if hasattr(vacancy, '_requirements') and vacancy._requirements:
        items = "\n".join(f"- {r}" for r in vacancy._requirements)
        req_block = f"\nТребования работодателя к письму:\n{items}\nОбязательно ответь на каждое требование.\n"

    prompt = EVALUATE_PROMPT.format(
        title=vacancy.title,
        company=vacancy.company or "Не указана",
        salary=vacancy.salary or "Не указана",
        city=vacancy.city or "Не указан",
        description=(vacancy.description or "")[:3000],
        min_score=min_score,
        requirements_block=req_block,
        candidate_name=candidate_name,
        candidate_profile=candidate_profile,
    )

    for attempt in range(2):
        try:
            response = await _client.messages.create(
                model=MODEL,
                max_tokens=800,
                system=SYSTEM_PROMPT,
                messages=[{"role": "user", "content": prompt}],
            )
            text = response.content[0].text
            result = _parse_json(text)
            result["score"] = max(0, min(100, int(result.get("score", 0))))
            result.setdefault("cover_letter", None)
            return result
        except Exception as e:
            log.warning(f"AI evaluate attempt {attempt + 1} failed: {e}")
            if attempt == 0:
                continue
            return {"score": 0, "reason": f"AI error: {e}", "cover_letter": None}


async def generate_cover_letter(
    vacancy: Vacancy,
    chat_id: str,
    requirements: List[str] | None = None,
    version: int = 1,
) -> str:
    candidate_name, candidate_profile = await get_candidate_info(chat_id)

    req_block = ""
    if requirements:
        items = "\n".join(f"- {r}" for r in requirements)
        req_block = f"\nТребования работодателя к письму:\n{items}\nОбязательно ответь на каждое требование.\n"

    ver_block = ""
    if version > 1:
        ver_block = (
            f"\nЭто вариант #{version}. Напиши письмо с другого ракурса, "
            f"выдели другие сильные стороны кандидата.\n"
        )

    prompt = COVER_LETTER_PROMPT.format(
        title=vacancy.title,
        company=vacancy.company or "Не указана",
        description=(vacancy.description or "")[:3000],
        requirements_block=req_block,
        version_block=ver_block,
        candidate_name=candidate_name,
        candidate_profile=candidate_profile,
    )

    try:
        response = await _client.messages.create(
            model=MODEL,
            max_tokens=500,
            messages=[{"role": "user", "content": prompt}],
        )
        return response.content[0].text.strip()
    except Exception as e:
        log.error(f"Cover letter generation failed: {e}")
        return ""


async def improve_cover_letter(
    text: str,
    vacancy: Vacancy,
    requirements: List[str] | None = None,
) -> str:
    req_block = ""
    if requirements:
        items = "\n".join(f"- {r}" for r in requirements)
        req_block = f"\nТребования работодателя к письму:\n{items}\n"

    prompt = IMPROVE_COVER_LETTER_PROMPT.format(
        title=vacancy.title,
        company=vacancy.company or "Не указана",
        description=(vacancy.description or "")[:3000],
        requirements_block=req_block,
        user_text=text,
    )

    try:
        response = await _client.messages.create(
            model=MODEL,
            max_tokens=500,
            messages=[{"role": "user", "content": prompt}],
        )
        return response.content[0].text.strip()
    except Exception as e:
        log.error(f"Cover letter improvement failed: {e}")
        return text


async def generate_reply(
    vacancy: Vacancy,
    history: List[Message],
    chat_id: str,
) -> str:
    candidate_name, candidate_profile = await get_candidate_info(chat_id)

    history_text = "\n".join(
        f"{'[Работодатель]' if m.direction == 'incoming' else '[Вы]'}: {m.text}"
        for m in history
    )
    last_incoming = next(
        (m.text for m in reversed(history) if m.direction == "incoming"), ""
    )

    prompt = REPLY_PROMPT.format(
        vacancy_title=vacancy.title if vacancy else "Не указана",
        company=vacancy.company if vacancy else "Не указана",
        vacancy_description=(vacancy.description or "")[:2000] if vacancy else "",
        conversation_history=history_text or "Нет истории",
        last_message=last_incoming or "Нет сообщения",
        candidate_name=candidate_name,
        candidate_profile=candidate_profile,
    )

    try:
        response = await _client.messages.create(
            model=MODEL,
            max_tokens=500,
            messages=[{"role": "user", "content": prompt}],
        )
        return response.content[0].text.strip()
    except Exception as e:
        log.error(f"Reply generation failed: {e}")
        return ""


async def improve_text(text: str) -> str:
    prompt = (
        f"Улучши этот текст ответа работодателю. "
        f"Сделай профессиональнее, конкретнее, убери воду. "
        f"Русский язык. Верни ТОЛЬКО текст ответа.\n\n{text}"
    )
    try:
        response = await _client.messages.create(
            model=MODEL,
            max_tokens=500,
            messages=[{"role": "user", "content": prompt}],
        )
        return response.content[0].text.strip()
    except Exception as e:
        log.error(f"Text improvement failed: {e}")
        return text


PARSE_RESUME_PROMPT = """Извлеки из резюме структурированную информацию. Верни ТОЛЬКО валидный JSON:
{
  "name": "Имя Фамилия",
  "position": "желаемая должность",
  "experience_years": 3,
  "skills": ["Python", "SQL", ...],
  "education": "краткое описание образования",
  "summary": "2-3 предложения — ключевые компетенции кандидата"
}
Если поле не найдено — null."""


async def parse_resume(file_bytes: bytes, filename: str) -> dict:
    """Парсинг резюме (PDF/DOCX) через Claude Vision/text."""
    import base64

    ext = filename.rsplit(".", 1)[-1].lower()

    if ext == "pdf":
        content = [
            {
                "type": "document",
                "source": {
                    "type": "base64",
                    "media_type": "application/pdf",
                    "data": base64.standard_b64encode(file_bytes).decode(),
                },
            },
            {"type": "text", "text": PARSE_RESUME_PROMPT},
        ]
    else:
        # DOCX и прочее — извлекаем текст
        text = _extract_text(file_bytes, ext)
        content = f"{PARSE_RESUME_PROMPT}\n\n---\n{text}"

    try:
        response = await _client.messages.create(
            model=MODEL,
            max_tokens=1000,
            messages=[{"role": "user", "content": content}],
        )
        raw = response.content[0].text.strip()
        raw = re.sub(r"^```json\s*|```$", "", raw, flags=re.MULTILINE).strip()
        return json.loads(raw)
    except Exception as e:
        log.error(f"Resume parsing failed: {e}")
        return {}


def _extract_text(file_bytes: bytes, ext: str) -> str:
    """Извлечение текста из DOCX."""
    if ext == "docx":
        try:
            import docx
            import io
            doc = docx.Document(io.BytesIO(file_bytes))
            return "\n".join(p.text for p in doc.paragraphs if p.text.strip())
        except ImportError:
            return "[python-docx не установлен — загрузите PDF]"
    return file_bytes.decode("utf-8", errors="replace")
