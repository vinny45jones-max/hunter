"""
Тест 4: парсинг JSON из AI-ответов + извлечение external_id из URL.
Тестирует внутренние функции, не требующие реального API.
"""
import pytest
from src.ai_filter import _parse_json
from src.scraper import _extract_external_id


class TestParseJson:
    def test_clean_json(self):
        result = _parse_json('{"score": 85, "reason": "Good match"}')
        assert result["score"] == 85
        assert result["reason"] == "Good match"

    def test_markdown_wrapped(self):
        text = '```json\n{"score": 70, "reason": "OK"}\n```'
        result = _parse_json(text)
        assert result["score"] == 70

    def test_markdown_no_lang(self):
        text = '```\n{"score": 60, "reason": "Decent"}\n```'
        result = _parse_json(text)
        assert result["score"] == 60

    def test_regex_fallback(self):
        text = 'Some text "score": 42, more "reason": "Low relevance" end'
        result = _parse_json(text)
        assert result["score"] == 42
        assert result["reason"] == "Low relevance"

    def test_regex_fallback_no_reason(self):
        text = 'blah "score": 10 blah'
        result = _parse_json(text)
        assert result["score"] == 10
        assert result["reason"] == "N/A"

    def test_unparseable_raises(self):
        with pytest.raises(ValueError, match="Cannot parse JSON"):
            _parse_json("This is not JSON at all")

    def test_nested_json(self):
        text = '{"score": 95, "reason": "Excellent match for CEO role", "extra": true}'
        result = _parse_json(text)
        assert result["score"] == 95

    def test_whitespace_json(self):
        text = '  \n  {"score": 77, "reason": "Good"}  \n  '
        result = _parse_json(text)
        assert result["score"] == 77


class TestExtractExternalId:
    def test_vakansiya_url(self):
        url = "https://rabota.by/vakansiya/12345678"
        assert _extract_external_id(url) == "12345678"

    def test_vacancy_url(self):
        url = "https://rabota.by/vacancy/87654321"
        assert _extract_external_id(url) == "87654321"

    def test_numeric_fallback(self):
        url = "https://rabota.by/job/99887766"
        assert _extract_external_id(url) == "99887766"

    def test_query_params(self):
        url = "https://rabota.by/vakansiya/11223344?from=search"
        assert _extract_external_id(url) == "11223344"

    def test_no_id(self):
        url = "https://rabota.by/search"
        assert _extract_external_id(url) is None

    def test_short_number_ignored(self):
        # Числа < 6 цифр не считаются external_id
        url = "https://rabota.by/page/123"
        assert _extract_external_id(url) is None


@pytest.mark.asyncio
async def test_candidate_info_appends_target_roles(init_db):
    from src import database
    from src.ai_filter import get_candidate_info
    await database.set_setting("42", "candidate_profile", "Профиль")
    _, profile = await get_candidate_info("42")
    assert profile == "Профиль"
    await database.set_setting("42", "target_roles", "от РОП до C-level")
    _, profile = await get_candidate_info("42")
    assert profile.startswith("Профиль") and "от РОП до C-level" in profile
