# Project Memory

Last updated: 2026-04-13

## Current State

The project has working local tests plus opt-in live integration tests.

### Test layers

1. Default local test suite:
   - command: `.\.venv\Scripts\python.exe -m pytest -q`
   - current result: `102 passed, 3 skipped`

2. Live Anthropic API tests:
   - command: `.\.venv\Scripts\python.exe -m pytest tests\test_ai_filter_live.py --live-api -q`
   - current result: `2 passed`

3. Live end-to-end pipeline test:
   - command: `.\.venv\Scripts\python.exe -m pytest tests\test_pipeline_live.py --live-api --live-web -q`
   - current result: `1 passed`
   - note: Playwright browser launch may require running outside the sandbox on Windows

## Changes Made

### 1. Live pytest support for Anthropic API

Updated `tests/conftest.py`:
- added `--live-api`
- added `live_api` marker
- live tests are skipped by default
- added fixture that resolves a real `ANTHROPIC_API_KEY` from env or `.env`

Added `tests/test_ai_filter_live.py`:
- checks that executive / AI transformation vacancies score higher than frontend
- checks that cover letter generation returns non-empty text

### 2. Live pytest support for browser/web tests

Updated `tests/conftest.py`:
- added `--live-web`
- added `live_web` marker
- web tests are skipped by default

Added `tests/test_pipeline_live.py`:
- real scrape from `rabota.by`
- fetches one real vacancy description
- runs real AI relevance scoring
- saves result into a temporary SQLite database
- keeps Telegram side effects out of the test

### 3. Playwright lifecycle fix

Updated `src/scraper.py`:
- added persistent `_playwright` handle
- `get_browser()` now reuses the started Playwright instance
- `close()` now closes browser and stops Playwright

Reason:
- removed Windows warnings about unclosed Playwright transports after live runs

## Files Changed

- `src/scraper.py`
- `tests/conftest.py`
- `tests/test_ai_filter_live.py`
- `tests/test_pipeline_live.py`

## Live Environment Notes

- A real `ANTHROPIC_API_KEY` is configured in local `.env`
- Live pipeline uses `rabota.by` plus Anthropic
- Browser-based live tests can fail inside sandbox because Playwright needs a real subprocess on Windows

## Product / Architecture Direction

Apify was discussed on 2026-04-13.

Current recommendation:
- do not migrate the whole project to Apify
- if needed, use Apify only for scraping (`parse_search_results` and `get_full_description`)
- keep AI scoring, DB, Telegram, inbox, reply, and apply logic in the current app

Why:
- Apify helps with browser runtime, proxies, scheduling, and scraping reliability
- it is not a good default replacement for stateful account flows

## Next Likely Step

If work continues, the next clean step is:
- convert `scripts/live_test_scraper.py` into `tests/test_scraper_live.py`

That would make all live checks run through pytest instead of ad-hoc scripts.
