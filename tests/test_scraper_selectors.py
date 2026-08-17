"""Регрессия: разметка выдачи rabota.by переехала с div на article.

Карточка теперь <article data-qa="vacancy-serp__vacancy">, поэтому селектор,
привязанный к тегу div, находил 0 вакансий и пайплайн молча слал "Найдено 0".
"""
import pytest

from src.scraper import SELECTORS

# Разметка выдачи после редизайна (magritte): карточка — article.
NEW_MARKUP = """
<article data-qa="vacancy-serp__vacancy" class="magritte-card___bhGKz">
  <div class="vacancy-card--n77Dj8">
    <span class="vacancy-name-wrapper--PSD">
      <a data-qa="serp-item__title" href="https://rabota.by/vacancy/136215401?query=director">Коммерческий директор</a>
    </span>
    <span data-qa="vacancy-serp__vacancy-employer">ООО Ромашка</span>
    <div data-qa="vacancy-serp__compensation">3 000 Br за месяц, на руки</div>
    <div data-qa="vacancy-serp__vacancy-address">Минск</div>
  </div>
</article>
"""

# Старая разметка — должна продолжать матчиться (селекторы обратно совместимы).
OLD_MARKUP = """
<div data-qa="vacancy-serp__vacancy" class="serp-item">
  <a data-qa="serp-item__title" href="/vakansiya/12345678">Директор по продажам</a>
</div>
"""


@pytest.fixture(scope="module")
def browser():
    playwright = pytest.importorskip("playwright.sync_api")
    try:
        with playwright.sync_playwright() as p:
            try:
                b = p.chromium.launch(channel="chrome", headless=True)
            except Exception:
                b = p.chromium.launch(headless=True)
            yield b
            b.close()
    except Exception as e:  # браузер не установлен в окружении
        pytest.skip(f"Playwright browser unavailable: {e}")


def _parse(browser, markup):
    page = browser.new_page()
    try:
        page.set_content(f"<html><body>{markup}</body></html>")
        cards = page.query_selector_all(SELECTORS["vacancy_card"])
        result = []
        for card in cards:
            title_el = card.query_selector(SELECTORS["vacancy_title"])
            if not title_el:
                continue
            result.append({
                "title": title_el.inner_text().strip(),
                "href": title_el.get_attribute("href"),
                "company": (lambda el: el.inner_text().strip() if el else None)(
                    card.query_selector(SELECTORS["vacancy_company"])
                ),
                "salary": (lambda el: el.inner_text().strip() if el else None)(
                    card.query_selector(SELECTORS["vacancy_salary"])
                ),
                "city": (lambda el: el.inner_text().strip() if el else None)(
                    card.query_selector(SELECTORS["vacancy_city"])
                ),
            })
        return result
    finally:
        page.close()


def test_new_article_markup_parsed(browser):
    parsed = _parse(browser, NEW_MARKUP)
    assert len(parsed) == 1, "карточка-article не распознана селектором vacancy_card"
    v = parsed[0]
    assert v["title"] == "Коммерческий директор"
    assert v["href"].endswith("query=director")
    assert v["company"] == "ООО Ромашка"
    assert v["salary"].startswith("3 000 Br")
    assert v["city"] == "Минск"


def test_old_div_markup_still_parsed(browser):
    parsed = _parse(browser, OLD_MARKUP)
    assert len(parsed) == 1
    assert parsed[0]["title"] == "Директор по продажам"
