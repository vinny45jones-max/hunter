"""Парсинг резюме (PDF/DOCX) и извлечение профиля через Claude API."""

import json
import io
from dataclasses import dataclass, field
from typing import Optional

import anthropic
import pdfplumber
from docx import Document

from src.config import settings, log

_client = anthropic.AsyncAnthropic(api_key=settings.anthropic_api_key, max_retries=5)
MODEL = "claude-sonnet-4-20250514"

EXTRACT_PROMPT = """Извлеки из резюме структурированный профиль кандидата.

Отвечай ТОЛЬКО валидным JSON (без markdown-блоков):
{{
  "name": "Имя Фамилия",
  "title": "Текущая должность / специализация",
  "summary": "Краткое описание опыта и компетенций (2-3 предложения)",
  "experience_years": <число или null>,
  "industries": ["отрасль1", "отрасль2"],
  "skills": ["навык1", "навык2", "навык3"],
  "search_keywords": ["ключевое слово для поиска вакансий 1", "ключевое слово 2", "ключевое слово 3"]
}}

search_keywords — слова/фразы для поиска вакансий на rabota.by, релевантные опыту кандидата.
Максимум 7 ключевых слов. Только на русском языке.

РЕЗЮМЕ:
{resume_text}
"""


@dataclass
class CandidateProfile:
    name: str
    title: str
    summary: str
    experience_years: Optional[int] = None
    industries: list[str] = field(default_factory=list)
    skills: list[str] = field(default_factory=list)
    search_keywords: list[str] = field(default_factory=list)


def extract_text_pdf(file_bytes: bytes) -> str:
    """Извлекает текст из PDF."""
    text_parts = []
    with pdfplumber.open(io.BytesIO(file_bytes)) as pdf:
        for page in pdf.pages:
            page_text = page.extract_text()
            if page_text:
                text_parts.append(page_text)
    return "\n".join(text_parts)


def extract_text_docx(file_bytes: bytes) -> str:
    """Извлекает текст из DOCX."""
    doc = Document(io.BytesIO(file_bytes))
    return "\n".join(p.text for p in doc.paragraphs if p.text.strip())


def extract_text(file_bytes: bytes, filename: str) -> str:
    """Определяет формат и извлекает текст."""
    lower = filename.lower()
    if lower.endswith(".pdf"):
        return extract_text_pdf(file_bytes)
    elif lower.endswith(".docx"):
        return extract_text_docx(file_bytes)
    else:
        raise ValueError(f"Неподдерживаемый формат: {filename}. Нужен PDF или DOCX.")


async def parse_resume(file_bytes: bytes, filename: str) -> CandidateProfile:
    """Парсит резюме и возвращает структурированный профиль."""
    text = extract_text(file_bytes, filename)
    if not text.strip():
        raise ValueError("Не удалось извлечь текст из файла. Проверьте, что файл не пустой.")

    log.info("resume_parser: извлечён текст (%d символов), отправляю в Claude", len(text))

    # обрезаем если слишком длинный (Claude справится, но экономим токены)
    if len(text) > 15000:
        text = text[:15000]

    resp = await _client.messages.create(
        model=MODEL,
        max_tokens=1024,
        messages=[{"role": "user", "content": EXTRACT_PROMPT.format(resume_text=text)}],
    )

    raw = resp.content[0].text.strip()
    # убираем markdown-обёртку если Claude всё-таки добавил
    if raw.startswith("```"):
        raw = raw.split("\n", 1)[1].rsplit("```", 1)[0].strip()

    data = json.loads(raw)

    return CandidateProfile(
        name=data.get("name", ""),
        title=data.get("title", ""),
        summary=data.get("summary", ""),
        experience_years=data.get("experience_years"),
        industries=data.get("industries", []),
        skills=data.get("skills", []),
        search_keywords=data.get("search_keywords", []),
    )
