import asyncio
import json

from bs4 import BeautifulSoup

from publisher_lakehouse.ingestion.browser.constants import NAV_WAIT
from publisher_lakehouse.ingestion.browser.session import (
    _accept_cookies,
    _solve_cf_on_page,
    _wait_ready,
)
from publisher_lakehouse.ingestion.errors import error_list
from publisher_lakehouse.ingestion.sciencedirect.extract import (
    _extract_token_and_pii,
    _parse_arp_authors,
)


async def _arp_api_xhr(page, pii: str, token: str) -> dict | None:
    api_url = f"https://www.sciencedirect.com/sdfe/arp/pii/{pii}/authors" f"?jwt={token}"
    escaped = api_url.replace("'", "\\'")

    js = f"""
    (function() {{
        try {{
            var xhr = new XMLHttpRequest();
            xhr.open('GET', '{escaped}', false);
            xhr.setRequestHeader('Accept', 'application/json, text/plain, */*');
            xhr.setRequestHeader('Referer', window.location.href);
            xhr.send(null);
            return xhr.status + '||' + xhr.responseText;
        }} catch(e) {{
            return 'ERROR||' + e.toString();
        }}
    }})()
    """

    try:
        raw = await page.evaluate(js)
        if not raw or not isinstance(raw, str):
            return None

        status_str, _, body = raw.partition("||")
        if status_str == "ERROR" or status_str != "200":
            return None
        if not body.strip().startswith("{"):
            return None

        return json.loads(body)
    except Exception:
        return None


async def _arp_api_browser_nav(browser, pii: str, token: str) -> dict | None:
    api_url = f"https://www.sciencedirect.com/sdfe/arp/pii/{pii}/authors" f"?jwt={token}"
    try:
        page = await browser.get(api_url)
        await _wait_ready(page, timeout=NAV_WAIT)
        await asyncio.sleep(1)

        html = await page.get_content()
        soup = BeautifulSoup(html, "lxml")
        raw_text = soup.get_text(strip=True)

        if not raw_text.startswith("{"):
            return None

        return json.loads(raw_text)
    except Exception:
        return None


async def extract_author_details(browser, html, article_url: str, page=None):
    try:
        token, pii, _ = _extract_token_and_pii(html, article_url)
        if not token or not pii:
            missing = "token" if not token else "pii"
            error_list.append(f"extract_author_details | no {missing} | {article_url}")
            return None, None
        if page is None:
            page = await browser.get(article_url)
            await _solve_cf_on_page(page)
            await _wait_ready(page, timeout=NAV_WAIT)
            await _accept_cookies(page)

        api_data = await _arp_api_xhr(page, pii, token)
        if api_data is None:
            api_data = await _arp_api_browser_nav(browser, pii, token)
        if api_data is None:
            return None, None

        author_results, corporate_results = _parse_arp_authors(api_data)
        return author_results, corporate_results

    except Exception as ex:
        error_list.append(f"extract_author_details | {article_url} | {ex}")
        return None, None
