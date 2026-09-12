import asyncio
import random
import time

import zendriver as zd
from bs4 import BeautifulSoup

from publisher_lakehouse.ingestion.browser.constants import (
    BROWSER_ARGS,
    DELAY_AFTER_NAV,
    NAV_WAIT,
    RETRY_BACKOFF_BASE,
    RETRY_MAX,
    USER_AGENT,
    WAIT_LONG,
    WAIT_SHORT,
)
from publisher_lakehouse.ingestion.errors import error_list


async def _solve_cf_on_page(page):
    # Spoof AFTER navigation, ON the landed page
    try:
        await page.set_user_agent(USER_AGENT)
        await page.evaluate("""
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']});
            Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
            Object.defineProperty(navigator, 'platform', {get: () => 'Win32'});
            window.chrome = {runtime: {}};
        """)
    except Exception:
        pass

    try:
        title = await page.evaluate("document.title")
    except Exception:
        title = ""

    if "just a moment" in title.lower():
        print("  🔄 Solving Cloudflare...")
        try:
            await page.verify_cf()
        except Exception:
            pass
        await asyncio.sleep(8)

    # Settle polling
    for _ in range(15):
        try:
            title = await page.evaluate("document.title")
            if "just a moment" not in title.lower():
                break
        except Exception:
            pass
        await asyncio.sleep(2)


async def start_browser():
    browser = await zd.start(
        headless=True,
        browser_args=list(BROWSER_ARGS),
    )
    return browser


async def _wait_ready(page, timeout=NAV_WAIT):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            state = await page.evaluate("document.readyState")
            if state == "complete":
                return True
        except Exception:
            pass
        await asyncio.sleep(0.2)
    return False


async def _accept_cookies(page):
    try:
        btn = await page.select("#onetrust-accept-btn-handler", timeout=WAIT_SHORT)
        if btn:
            try:
                await btn.click()
            except Exception:
                await page.evaluate("document.getElementById('onetrust-accept-btn-handler')?.click()")
            await asyncio.sleep(0.5)
    except Exception:
        pass


def is_blocked(title: str) -> bool:
    if not title:
        return True
    t = title.strip().lower()
    if t == "sciencedirect":  # bare title = soft-blocked, no article loaded
        return True
    blocked_phrases = [
        "just a moment",
        "access denied",
        "are you a robot",
        "403 forbidden",
        "robot check",
        "ddos protection",
        "checking your browser",
    ]
    return any(phrase in t for phrase in blocked_phrases)


async def setup_browser_session(browser, seed_url: str) -> None:
    print("  [setup] Warming up browser session...")
    try:
        page = await browser.get(seed_url)
        try:
            await page.set_user_agent(USER_AGENT)
        except Exception:
            pass
        try:
            await page.evaluate("""
                Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
                Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']});
                Object.defineProperty(navigator, 'plugins',   {get: () => [1, 2, 3, 4, 5]});
                Object.defineProperty(navigator, 'platform',  {get: () => 'Win32'});
                window.chrome = {runtime: {}};
            """)
        except Exception:
            pass

        await asyncio.sleep(5)

        title = await page.evaluate("document.title")
        if "just a moment" in title.lower():
            print("  [setup] CF detected — solving...")
            try:
                await page.verify_cf()
            except Exception:
                pass
            await asyncio.sleep(8)

        # Same 15-iteration poll as check_script.py
        for _ in range(15):
            try:
                title = await page.evaluate("document.title")
                if "just a moment" not in title.lower():
                    break
            except Exception:
                pass
            await asyncio.sleep(2)

        print(f"  [setup] Session ready. Title: '{title[:80]}'")

    except Exception as e:
        error_list.append(f"setup_browser_session | {seed_url} | {e}")
        print(f"  [setup] Warning: setup failed ({e})")


async def fetch_soup(browser, url, wait_id=None, retries=RETRY_MAX):
    last_exc = None
    for attempt in range(1, retries + 1):
        try:
            page = await browser.get(url)
            await _solve_cf_on_page(page)  # ← inline, no monkey-patch needed
            await _wait_ready(page, timeout=NAV_WAIT)
            await _accept_cookies(page)

            if wait_id:
                try:
                    await page.select(f"#{wait_id}", timeout=WAIT_LONG)
                except Exception:
                    pass  # Selector didn't match — page may still be usable

            await asyncio.sleep(random.uniform(*DELAY_AFTER_NAV))

            html = await page.get_content()
            return BeautifulSoup(html, "lxml")

        except Exception as e:
            last_exc = e
            if attempt < retries:
                backoff = RETRY_BACKOFF_BASE**attempt + random.uniform(0, 1)
                print(f"  [fetch retry {attempt}/{retries}] {url[:80]}... {type(e).__name__}: {e}; sleeping {backoff:.1f}s")
                await asyncio.sleep(backoff)

    message = f"fetch_soup failed after {retries} attempts | {url} | {type(last_exc).__name__}: {last_exc}"
    error_list.append(message)
    print(f"  [fetch failed] {message}")
    return None


async def fetch_soup_with_page(browser, url, wait_id=None, retries=RETRY_MAX):
    last_exc = None
    for attempt in range(1, retries + 1):
        try:
            page = await browser.get(url)
            await _solve_cf_on_page(page)
            await _wait_ready(page, timeout=NAV_WAIT)
            await _accept_cookies(page)

            if wait_id:
                try:
                    await page.select(f"#{wait_id}", timeout=WAIT_LONG)
                except Exception:
                    pass

            await asyncio.sleep(random.uniform(*DELAY_AFTER_NAV))

            html = await page.get_content()
            return BeautifulSoup(html, "lxml"), page, html

        except Exception as e:
            last_exc = e
            if attempt < retries:
                backoff = RETRY_BACKOFF_BASE**attempt + random.uniform(0, 1)
                print(f"  [fetch retry {attempt}/{retries}] {url[:80]}... {type(e).__name__}: {e}; sleeping {backoff:.1f}s")
                await asyncio.sleep(backoff)

    message = f"fetch_soup_with_page failed after {retries} attempts | {url} | {type(last_exc).__name__}: {last_exc}"
    error_list.append(message)
    print(f"  [fetch failed] {message}")
    return None, None, None
