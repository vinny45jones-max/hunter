import pytest


pytestmark = [pytest.mark.asyncio, pytest.mark.live_api, pytest.mark.live_web]


async def _no_sleep(*args, **kwargs):
    return None


async def test_live_pipeline_scrape_ai_db(
    tmp_path,
    monkeypatch,
    live_anthropic_api_key,
    live_web_enabled,
):
    from src import browser_pool, database, pipeline, scraper

    test_db = str(tmp_path / "live_pipeline.db")
    monkeypatch.setattr(database, "_db_path", test_db)

    async def _parse_one_live_vacancy(chat_id, *args, **kwargs):
        vacancies = await scraper.parse_search_results("директор", chat_id, max_pages=1)
        return vacancies[:1]

    monkeypatch.setattr(scraper, "_random_delay", _no_sleep)
    monkeypatch.setattr(pipeline.asyncio, "sleep", _no_sleep)
    monkeypatch.setattr(pipeline.scraper, "parse_all_keywords", _parse_one_live_vacancy)
    monkeypatch.setattr(pipeline.settings, "min_relevance_score", 101)

    await database.init()

    try:
        await pipeline.run_pipeline()

        last = await database.get_last_vacancies(5)
        assert last, "pipeline did not save any vacancies"

        saved = await database.get_vacancy(last[0].id)
        assert saved is not None
        assert saved.external_id
        assert saved.url.startswith("http")
        assert saved.title.strip()
        assert saved.description
        assert len(saved.description.strip()) >= 200
        assert 0 <= saved.relevance_score <= 100
        assert saved.relevance_reason
        assert not saved.relevance_reason.startswith("AI error:")
        assert saved.status == "new"

        stats = await database.get_stats()
        assert stats["total"] >= 1
    finally:
        await browser_pool.close()
