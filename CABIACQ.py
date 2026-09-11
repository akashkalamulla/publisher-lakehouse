import asyncio
import base64
import json
import os
import random
import re
import sys
import time
from datetime import datetime
from urllib.parse import unquote, urlparse

import xlsxwriter
import zendriver as zd
from bs4 import BeautifulSoup, FeatureNotFound, NavigableString

# ============================================================================
# CONFIG
# ============================================================================
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36"

BROWSER_ARGS = [
    "--no-sandbox",
    "--disable-blink-features=AutomationControlled",
    "--disable-dev-shm-usage",
    "--window-size=1280,800",
    "--window-position=-32000,-32000",
    f"--user-agent={USER_AGENT}",
]

# Rate limiting
DELAY_BETWEEN_ARTICLES = (3.0, 7.0)  # seconds (random in range)
DELAY_AFTER_NAV = (0.5, 1.5)

# Retries
RETRY_MAX = 3
RETRY_BACKOFF_BASE = 2.0  # 2s, 4s, 8s ...

# Timeouts (seconds)
NAV_WAIT = 30
WAIT_LONG = 15
WAIT_SHORT = 3
ARTICLE_TIMEOUT = 300  # 5 min max per article before treating as stuck
MAX_TIMEOUT_RETRIES = 3  # attempts before giving up on that article
BLOCK_RECOVERY_TIMEOUT = 300
DATE = datetime.now()

# Output
import configparser

config = configparser.ConfigParser()
config.read("config.ini")
base_out_path = config.get("DETAILS", "output_path", fallback=os.getcwd()) + "//out"
skip_issues = "In progress"
error_list: list[str] = []


def _log_missing(field: str, article_url: str, title, journal) -> None:
    msg = f"[MISSING] {field} | url: {article_url} | " f"title: {title or 'N/A'} | journal: {journal or 'N/A'}"
    error_list.append(msg)
    print(f"  ⚠️  MISSING {field} — {article_url}")


folder_path = os.path.join(base_out_path, DATE.strftime("%Y%m%d"))


# ============================================================================
# BROWSER HELPERS
# ============================================================================
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


# ============================================================================
# ARP API AUTHOR EXTRACTION
# ============================================================================
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


def _find_token(obj, depth=0):
    if depth > 6:
        return None
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k == "authorToken" and isinstance(v, str) and v:
                return v
            r = _find_token(v, depth + 1)
            if r:
                return r
    elif isinstance(obj, list):
        for item in obj:
            r = _find_token(item, depth + 1)
            if r:
                return r
    return None


def _extract_token_and_pii(html, article_url: str):
    pii_match = re.search(r"/pii/([A-Z0-9]+)", article_url, re.IGNORECASE)
    pii = pii_match.group(1) if pii_match else None

    state_match = re.search(
        r"window\.__PRELOADED_STATE__\s*=\s*(\{.*?\});\s*</script>",
        html,
        re.DOTALL,
    )
    if not state_match:
        return None, pii, None
    try:
        data = json.loads(state_match.group(1))
        token = _find_token(data)
        dates = data.get("article", {}).get("dates", {})
        return token, pii, dates
    except Exception:
        return None, pii, None


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


def _decode_email(encoded: str) -> str:
    try:
        padded = encoded + "=" * (-len(encoded) % 4)
        data = json.loads(unquote(base64.b64decode(padded).decode("utf-8")))
        email = data.get("_", "").strip()
        if not email:
            href = data.get("$", {}).get("href", "")
            if href.startswith("mailto:"):
                email = href[7:].strip()
        return email
    except Exception:
        return ""


def _collect_authors(items):
    out = []
    for item in items:
        if not isinstance(item, dict):
            continue
        tag = item.get("#name", "")
        if tag == "author":
            out.append(item)
        elif tag in ("author-group", "writing-group", "collaboration"):
            out.extend(_collect_authors(item.get("$$", [])))
    return out


# Affiliation refids appear in two formats across ScienceDirect journals:
#   - "af0005" style — two letters + zero-padded digits (current ARP API)
#   - "aff5"   style — three letters + plain digits (older / other layouts)
# Correspondence refs ("cr0005", "cor1") and other ref types must be excluded.
_AFF_REF_RE = re.compile(r"^aff?\d+$")


def _is_affiliation_ref(refid: str, aff_map: dict) -> bool:
    if not refid:
        return False
    # Ground truth: refid points to a real entry in the affiliations map.
    # This alone covers both formats whenever the map keys match the refids.
    if refid in aff_map:
        return True
    # Format fallback: "af?" makes the 2nd 'f' optional, so it matches both
    # "af0005" and "aff5" — but never "cr..."/"cor..." correspondence refs.
    return bool(_AFF_REF_RE.match(refid))


def _collect_collaborations(items):
    """Return list of top-level collaboration nodes from the author-group $$."""
    out = []
    for item in items:
        if not isinstance(item, dict):
            continue
        if item.get("#name") == "collaboration":
            out.append(item)
    return out


def _clean_affiliation_text(text: str) -> str:
    if not text:
        return text
    cleaned = re.compile(r"\s*/\s*").sub(" ", text)
    cleaned = re.compile(r",\s*,+").sub(" ", cleaned)
    return re.sub(r"\s+", " ", cleaned).strip()


def _parse_arp_authors(api_data: dict):
    aff_map: dict[str, str] = {}
    for aff_id, val in api_data.get("affiliations", {}).items():
        if not isinstance(val, dict):
            continue
        for sub in val.get("$$", []):
            if sub.get("#name") == "textfn":
                text = _clean_affiliation_text(sub.get("_", "").strip())
                if text:
                    aff_map[aff_id] = text
                break

    try:
        for item in api_data.get("content", [{}])[0].get("$$", []):
            if item.get("#name") != "affiliation":
                continue
            aff_id = item.get("$", {}).get("id", "")
            if not aff_id or aff_id in aff_map:
                continue
            for sub in item.get("$$", []):
                if sub.get("#name") == "textfn":
                    text = _clean_affiliation_text(sub.get("_", "").strip())
                    if text:
                        aff_map[aff_id] = text
                    break
    except Exception:
        pass

    root_items = api_data.get("content", [{}])[0].get("$$", [])
    raw_authors = _collect_authors(root_items)

    # ── Pass 1: collect raw data per author ─────────────────────────────────
    intermediate = []  # list of (given, surname, email, aff_ids)
    for author in raw_authors:
        given = surname = email = ""
        aff_ids = []
        for f in author.get("$$", []):
            tag = f.get("#name", "")
            if tag == "given-name":
                given = f.get("_", "").strip()
            elif tag == "surname":
                surname = f.get("_", "").strip()
            elif tag == "cross-ref":
                refid = f.get("$", {}).get("refid", "")
                if _is_affiliation_ref(refid, aff_map):
                    aff_ids.append(refid)
            elif tag == "encoded-e-address":
                enc = f.get("__encoded", "")
                if enc:
                    email = _decode_email(enc)
        intermediate.append((given, surname, email, aff_ids))

    # ── Single shared affiliation rule ───────────────────────────────────────
    # When every author has zero affiliation cross-refs AND there is exactly
    # one affiliation in the map, ScienceDirect omits superscripts entirely —
    # the single affiliation is implicitly shared by all authors.
    all_unlinked = all(len(aff_ids) == 0 for _, _, _, aff_ids in intermediate)
    if all_unlinked and len(aff_map) == 1:
        shared_aff_ids = list(aff_map.keys())
    else:
        shared_aff_ids = []

    # ── Pass 2: build final results ──────────────────────────────────────────
    results = []
    for given, surname, email, aff_ids in intermediate:
        effective_ids = shared_aff_ids if shared_aff_ids else aff_ids
        name = f"{given} {surname}".strip()
        affs = [aff_map.get(a, f"[MISSING:{a}]") for a in effective_ids]
        aff_str = "\\".join([a for a in affs if a])
        results.append([name, aff_str, email])

    # ── Collaboration (corporate author) extraction ──────────────────────────
    corporate_results = []
    collab_nodes = _collect_collaborations(root_items)
    for collab in collab_nodes:
        collab_name = ""
        collab_email = ""
        collab_aff_ids = []
        for child in collab.get("$$", []):
            tag = child.get("#name", "")
            if tag == "text":
                collab_name = child.get("_", "").strip()
            elif tag == "encoded-e-address":
                enc = child.get("__encoded", "")
                if enc:
                    collab_email = _decode_email(enc)
            elif tag == "cross-ref":
                refid = child.get("$", {}).get("refid", "")
                if _is_affiliation_ref(refid, aff_map):
                    collab_aff_ids.append(refid)
        collab_affs = [aff_map.get(a, f"[MISSING:{a}]") for a in collab_aff_ids]
        collab_aff_str = "\\".join([a for a in collab_affs if a])
        if collab_name:
            corporate_results.append([collab_name, collab_aff_str, collab_email])

    return results, corporate_results


# ============================================================================
# HTML → RICH TEXT (for Excel write_rich_string)
# ============================================================================
def html_to_rich_text(element, workbook):
    runs = []
    plain_text = ""
    has_formatting = False

    for child in element.descendants:
        if isinstance(child, NavigableString):
            text = str(child)
            if not text:
                continue

            bold = italic = superscript = subscript = False
            parent = child.parent
            while parent and parent != element.parent:
                tag = parent.name
                if tag in ("b", "strong"):
                    bold = True
                elif tag in ("i", "em"):
                    italic = True
                elif tag == "sup":
                    superscript = True
                elif tag == "sub":
                    subscript = True
                parent = parent.parent

            if bold or italic or superscript or subscript:
                has_formatting = True

            fmt_props = {}
            if bold:
                fmt_props["bold"] = True
            if italic:
                fmt_props["italic"] = True
            if superscript:
                fmt_props["font_script"] = 1
            if subscript:
                fmt_props["font_script"] = 2

            fmt = workbook.add_format(fmt_props)
            runs.append(fmt)
            runs.append(text)
            plain_text += text

    plain_text = plain_text.strip()
    if has_formatting and len(runs) >= 4:
        return runs, plain_text
    return None, plain_text


# ============================================================================
# PARSERS — pure BeautifulSoup, no browser needed
# ============================================================================
def get_latest_issue_url(soup):
    try:
        main_div = soup.find("div", {"id": "all-issues"})
        latest_issue_set = main_div.find("li", {"class": "accordion-panel"})
        latest_issue_row = latest_issue_set.find("div", {"class": "issue-item u-margin-s-bottom"})
        latest_issue_text = str(latest_issue_row.text)

        if str(skip_issues) in latest_issue_text:
            return "Skip"

        issue_link = latest_issue_row.find("a", {"class": "anchor js-issue-item-link text-l anchor-primary"})["href"]
        return issue_link

    except Exception as ex:
        error_list.append(f"get_latest_issue_url | {ex}")
        return None


def _volume_issue_from_url(issue_url):
    if not issue_url:
        return None, None
    m = re.search(r"/vol/(\d+)/(?:issue/(\d+(?:-\d+)?)|suppl/)", issue_url, re.IGNORECASE)
    if not m:
        return None, None
    return m.group(1), m.group(2)


def extract_issue_volume_pubdate(soup, issue_url=None):
    try:
        vol_issue = soup.find("h2", {"class": ["u-text-light issue-info-heading js-vol-issue", "u-text-light js-special-issue-title js-title"]})
        vol_issue_row = vol_issue.text

        vol_match = re.search(r"volume\s*(\d+)", vol_issue_row, re.IGNORECASE)

        issue_match = re.search(
            r"Issues?\s*(\d+(?:[–-]\d+)?)",
            vol_issue_row,
            re.IGNORECASE,
        )

        volume = vol_match.group(1) if vol_match else None
        issue = issue_match.group(1) if issue_match else None

        pub_data_txt = vol_issue.find_next("div", {"class": "js-issue-status text-s"}).text
        year_match = re.search(r"\b(19|20)\d{2}\b", pub_data_txt)
        pub_year = year_match.group(0) if year_match else None

        MONTHS = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"]

        matches = re.findall(r"([A-Za-z]+(?:–[A-Za-z]+)?)\s+\d{4}", pub_data_txt)

        pub_month = None

        for match in reversed(matches):
            first_month = match.split("–")[0]

            if first_month in MONTHS:
                pub_month = match
                break

        if volume is None and issue is None:
            v_url, i_url = _volume_issue_from_url(issue_url)
            volume = v_url
            issue = i_url

        return volume, issue, pub_year, pub_month

    except Exception as ex:
        error_list.append(f"extract_issue_volume_pubdate | {ex}")
        return None, None, None, None


def get_all_article_list(soup):
    try:
        main_list = soup.find("ol", {"class": "js-article-list article-list-items"})
        article_list = main_list.find_all(
            "li",
            {"class": "js-article-list-item article-item u-padding-xs-top u-margin-l-bottom"},
        )
        return article_list
    except Exception as ex:
        error_list.append(f"get_all_article_list | {ex}")
        return None


def extract_article_data(soup):
    try:
        MONTHS = "January|February|March|April|May|June|July|" "August|September|October|November|December"

        result = {
            "publication_day": None,
            "publication_month": None,
            "publication_year": None,
            "start_page": None,
            "end_page": None,
            "article_id": None,
        }

        pub_data_row = soup.find("div", {"class": "publication-volume u-text-center"})
        third_method = True
        if pub_data_row:
            third_method = False
            pub_data_txt = pub_data_row.find("div", {"class": "text-xs"}).text

            m = re.search(rf"\b(\d{{1,2}})\s+({MONTHS})\s+(\d{{4}})\b", pub_data_txt)
            if m:
                result["publication_day"] = int(m.group(1))
                result["publication_month"] = m.group(2)
                result["publication_year"] = int(m.group(3))
            else:
                m = re.search(
                    rf"\b({MONTHS})(?:[–-]({MONTHS}))?\s+(\d{{4}})\b",
                    pub_data_txt,
                    re.IGNORECASE,
                )
                if m:
                    first_month = m.group(1)
                    second_month = m.group(2)
                    year = m.group(3)

                    if second_month:
                        result["publication_month"] = f"{first_month}-{second_month}"
                    else:
                        result["publication_month"] = first_month
                    result["publication_year"] = int(m.group(3))
                else:
                    m = re.search(r",\s*((?:19|20)\d{2})\s*,", pub_data_txt)

                    if m:
                        result["publication_year"] = int(m.group(1))

            m = re.search(
                r"\bPages?\s+([A-Za-z0-9]+)\s*-\s*([A-Za-z0-9]+)\b",
                pub_data_txt,
                re.IGNORECASE,
            )
            if m:
                result["start_page"] = m.group(1)
                result["end_page"] = m.group(2)

            elif m := re.search(
                r"\bPage\s+([A-Za-z0-9]+)\b",
                pub_data_txt,
                re.IGNORECASE,
            ):
                result["start_page"] = m.group(1)

            clean_txt = re.sub(r"\b(?:Volume|Issue)\s+\d+\b", "", pub_data_txt, flags=re.IGNORECASE)
            nums = re.findall(r"\b\d{5,}\b", clean_txt)
            if nums:
                result["article_id"] = nums[-1]
        else:
            pub_data_row = soup.find("div", {"class": "publication-metadata"})
            if pub_data_row:
                third_method = False
                date_label = soup.find(lambda tag: tag.name == "span" and "u-text-bold" in tag.get("class", []) and "Date" in tag.get_text(" ", strip=True))
                if date_label:
                    date_text = date_label.parent.get_text()

                    date_match = re.search(
                        r"(?:(\d{1,2})\s+)?([A-Za-z]+(?:[–-][A-Za-z]+)?)\s+(\d{4})",
                        date_text,
                    )

                    if date_match:
                        result["publication_day"] = date_match.group(1)
                        result["publication_month"] = date_match.group(2)
                        result["publication_year"] = date_match.group(3)
                    else:
                        date_match = re.search(
                            r"Date\s+(\d{4})",
                            date_text,
                        )
                        if date_match:
                            result["publication_year"] = date_match.group(1)

                page_label = soup.find(lambda tag: tag.name == "span" and "u-text-bold" in tag.get("class", []) and "Page" in tag.get_text(" ", strip=True))
                if page_label:
                    page_text = page_label.parent.get_text()
                    range_match = re.search(
                        r"\bPages?:?\s*([A-Za-z0-9]+)\s*-\s*([A-Za-z0-9]+)\b",
                        page_text,
                        re.IGNORECASE,
                    )
                    if range_match:
                        result["start_page"] = range_match.group(1)
                        result["end_page"] = range_match.group(2)

                    else:
                        single_match = re.search(
                            r"\bPages?:?\s*([A-Za-z0-9]+)\b",
                            page_text,
                            re.IGNORECASE,
                        )
                        if single_match:
                            result["start_page"] = single_match.group(1)
                            result["end_page"] = None

                article_label = soup.find(lambda tag: tag.name == "span" and "u-text-bold" in tag.get("class", []) and "Article" in tag.get_text(" ", strip=True))
                if article_label:
                    article_text = article_label.parent.get_text()

                    match = re.search(r"Article:\s*(\d+)", article_text)
                    if match:
                        result["article_id"] = match.group(1)

        if third_method:
            issue_link_tag = soup.find("a", attrs={"title": "Go to table of contents for this volume/issue"})
            if issue_link_tag:
                parent = issue_link_tag.parent

                if parent.name == "div" and "text-xs" in parent.get("class", []):
                    pub_data_txt = parent.text

                    m = re.search(rf"\b(\d{{1,2}})\s+({MONTHS})\s+(\d{{4}})\b", pub_data_txt)
                    if m:
                        result["publication_day"] = int(m.group(1))
                        result["publication_month"] = m.group(2)
                        result["publication_year"] = int(m.group(3))
                    else:
                        m = re.search(
                            rf"\b({MONTHS})(?:[–-]({MONTHS}))?\s+(\d{{4}})\b",
                            pub_data_txt,
                            re.IGNORECASE,
                        )
                        if m:
                            first_month = m.group(1)
                            second_month = m.group(2)
                            year = m.group(3)

                            if second_month:
                                result["publication_month"] = f"{first_month}-{second_month}"
                            else:
                                result["publication_month"] = first_month
                            result["publication_year"] = int(m.group(3))
                        else:
                            m = re.search(r",\s*((?:19|20)\d{2})\s*,", pub_data_txt)

                            if m:
                                result["publication_year"] = int(m.group(1))

                    m = re.search(
                        r"\bPages?\s+([A-Za-z0-9]+)\s*-\s*([A-Za-z0-9]+)\b",
                        pub_data_txt,
                        re.IGNORECASE,
                    )
                    if m:
                        result["start_page"] = m.group(1)
                        result["end_page"] = m.group(2)

                    elif m := re.search(
                        r"\bPage\s+([A-Za-z0-9]+)\b",
                        pub_data_txt,
                        re.IGNORECASE,
                    ):
                        result["start_page"] = m.group(1)

            clean_txt = re.sub(r"\b(?:Volume|Issue)\s+\d+\b", "", pub_data_txt, flags=re.IGNORECASE)
            nums = re.findall(r"\b\d{5,}\b", clean_txt)
            if nums:
                result["article_id"] = nums[-1]

        return result

    except Exception as ex:
        error_list.append(f"extract_article_data | {ex}")
        return None


def extract_main_title(soup):
    try:
        title = soup.find("a", {"id": "journal-title"}).text
        title = title.strip()

        return title

    except Exception as ex:
        error_list.append(f"extract_main_title | {ex}")
        return None


def extract_article_titles(soup, workbook):
    try:
        eng_rich = eng_plain = for_rich = for_plain = None

        # Strategy 1
        article_class = soup.find("h1", {"id": "screen-reader-main-title"})
        if article_class is not None:
            for span in article_class.find_all("span"):
                classes = span.get("class", [])
                if "article-alt-title" in classes:
                    alt_lang = span.get("lang", "en").lower()
                    ft_span = article_class.find("span", {"class": "title-text"})
                    if alt_lang == "en" or alt_lang == "":
                        # alt-title is the English translation,
                        # title-text is the original language title.
                        eng_rich, eng_plain = html_to_rich_text(span, workbook)
                        if ft_span:
                            for_rich, for_plain = html_to_rich_text(ft_span, workbook)
                    else:
                        # alt-title is the foreign language title,
                        # title-text is the English title.
                        if ft_span:
                            eng_rich, eng_plain = html_to_rich_text(ft_span, workbook)
                        for_rich, for_plain = html_to_rich_text(span, workbook)
                    return eng_rich, eng_plain, for_rich, for_plain

            title_span = article_class.find("span", {"class": "title-text"})
            if title_span:
                eng_rich, eng_plain = html_to_rich_text(title_span, workbook)
                return eng_rich, eng_plain, for_rich, for_plain

        # Strategy 2
        for h1 in soup.find_all("h1"):
            title_span = h1.find("span", {"class": "title-text"})
            if title_span:
                eng_rich, eng_plain = html_to_rich_text(title_span, workbook)
                return eng_rich, eng_plain, for_rich, for_plain

        # Strategy 3
        # for h1 in soup.find_all("h1"):
        #    text = h1.get_text(" ", strip=True)
        #    if text:
        #        return None, text, None, None

        # Strategy 4
        title_tag = soup.find("title")
        if title_tag:
            t = title_tag.get_text(" ", strip=True)
            t = re.sub(r"\s*-\s*ScienceDirect\s*$", "", t).strip()
            if t:
                return None, t, None, None

        return None, None, None, None

    except Exception as ex:
        error_list.append(f"extract_article_titles | {ex}")
        return None, None, None, None


def extract_doi(soup):
    try:
        doi_row = soup.find("a", {"class": "anchor doi anchor-primary"})
        if doi_row:
            doi_row = soup.find("a", {"class": "anchor doi anchor-primary"}).text
        else:
            doi_row = soup.find("a", {"aria-describedby": "doi-link-tooltip"}).text
            if doi_row:
                doi_row = soup.find("a", {"aria-describedby": "doi-link-tooltip"}).text
            else:
                doi_row = ""
        return str(doi_row).replace("https://doi.org/", "").strip()
    except Exception as ex:
        error_list.append(f"extract_doi | {ex}")
        return None


def extract_abstract(soup, workbook=None):
    try:
        abstracts_div = soup.find("div", {"id": "abstracts"})
        if not abstracts_div:
            return None, None

        abstract_section = None
        SKIP_CLASSES = {"author-highlights", "graphical"}
        SKIP_HEADINGS = {"highlights", "Highlights"}
        NON_ENGLISH_HEADINGS = {"résumé", "resumen", "zusammenfassung", "摘要", "resumo", "riassunto"}

        def _is_real_abstract(div):
            if any(c in SKIP_CLASSES for c in div.get("class", [])):
                return False
            h2 = div.find("h2")
            if h2 and h2.get_text(strip=True).lower() in SKIP_HEADINGS:
                return False
            return True

        # ── 1. Prefer a div whose <h2> literally says "Abstract" ─────────────
        for div in abstracts_div.find_all("div", recursive=False):
            if not _is_real_abstract(div):
                continue
            h2 = div.find("h2")
            if h2 and h2.get_text(strip=True).lower() == "abstract":
                abstract_section = div
                break

        # ── 2. Fallback: first lang="en" div that isn't highlights/graphical ─
        if abstract_section is None:
            for div in abstracts_div.find_all("div", recursive=False):
                if not _is_real_abstract(div):
                    continue
                if div.get("lang", "").lower() == "en":
                    abstract_section = div
                    break

        # ── 3. Fallback: no lang attr + h2 not a known foreign heading ───────
        if abstract_section is None:
            for div in abstracts_div.find_all("div", recursive=False):
                if not _is_real_abstract(div):
                    continue
                h2 = div.find("h2")
                lang_attr = div.get("lang", "").lower()
                heading = h2.get_text(strip=True).lower() if h2 else ""
                if lang_attr not in ("", "en"):
                    continue
                if heading in NON_ENGLISH_HEADINGS:
                    continue
                abstract_section = div
                break

        if abstract_section is None:
            return None, None

        # ── Rich-text builder — no-op when workbook is None ──────────────────
        def _build_rich(blocks):
            if workbook is None or not blocks:
                return None
            seg_runs_list, any_fmt = [], False
            for heading, body in blocks:
                runs = []
                head = re.sub(r"\s+", " ", heading).strip() if heading else ""
                if head:
                    runs.append(head + "\n")
                b_runs, b_plain = html_to_rich_text(body, workbook)
                if b_runs is None:
                    bp = re.sub(r"\s+", " ", b_plain).strip()
                    if bp:
                        runs.append(bp)
                else:
                    any_fmt = True
                    for i in range(0, len(b_runs), 2):
                        fmt, txt = b_runs[i], re.sub(r"\s+", " ", b_runs[i + 1])
                        if txt:
                            runs.append(fmt)
                            runs.append(txt)
                if any(isinstance(r, str) for r in runs):
                    seg_runs_list.append(runs)
            if not any_fmt:
                return None
            master = []
            for i, runs in enumerate(seg_runs_list):
                if i > 0:
                    master.append("\n\n")
                master.extend(runs)
            merged = []
            for frag in master:
                if isinstance(frag, str) and merged and isinstance(merged[-1], str):
                    merged[-1] += frag
                else:
                    merged.append(frag)
            if merged and isinstance(merged[0], str):
                merged[0] = merged[0].lstrip()
            if merged and isinstance(merged[-1], str):
                merged[-1] = merged[-1].rstrip()
            merged = [f for f in merged if not (isinstance(f, str) and not f)]
            return merged if len(merged) >= 2 else None

        # ── Non-English sibling: resolved once, shared by all three steps ─────
        ne_div = None
        for div in abstracts_div.find_all("div", recursive=False):
            if div is abstract_section:
                continue
            h2 = div.find("h2")
            if h2 and h2.get_text(strip=True).lower() in NON_ENGLISH_HEADINGS:
                ne_div = div
                break

        non_english_abstract = None
        ne_rich = None
        if ne_div:
            ne_paras = ne_div.find_all("div", {"class": "u-margin-s-bottom"})
            if ne_paras:
                non_english_abstract = re.sub(r"\s+", " ", "\n\n".join(p.get_text(" ", strip=True) for p in ne_paras if p.get_text(strip=True))).strip()
                ne_rich = _build_rich([(None, p) for p in ne_paras if p.get_text(strip=True)])

        # ── Step 1: structured abstract (h3 headings) ────────────────────────
        h3_tags = abstract_section.find_all("h3")
        if h3_tags:
            parts, blocks = [], []
            for h3 in h3_tags:
                heading = h3.get_text(strip=True)
                body_div = h3.find_next_sibling("div")
                body = re.sub(r"\s+", " ", body_div.get_text(" ", strip=True)).strip() if body_div else ""
                if heading and body:
                    parts.append(f"{heading}\n{body}")
                elif body:
                    parts.append(body)
                if body_div:
                    blocks.append((heading or None, body_div))
            if parts:
                english_abstract = "\n\n".join(parts)
                eng_rich = _build_rich(blocks)
                return (eng_rich if eng_rich is not None else english_abstract, ne_rich if ne_rich is not None else non_english_abstract)

        # ── Step 2: plain paragraphs ──────────────────────────────────────────
        paras = abstract_section.find_all("div", {"class": "u-margin-s-bottom"})
        if paras:
            english_abstract = re.sub(r"\s+", " ", "\n\n".join(p.get_text(" ", strip=True) for p in paras if p.get_text(strip=True))).strip()
            blocks = [(None, p) for p in paras if p.get_text(strip=True)]
            eng_rich = _build_rich(blocks)
            return (eng_rich if eng_rich is not None else english_abstract, ne_rich if ne_rich is not None else non_english_abstract)

        # ── Step 3: last resort ───────────────────────────────────────────────
        english_abstract = re.sub(r"\s+", " ", abstract_section.get_text(" ", strip=True)).strip() or None
        eng_rich = _build_rich([(None, abstract_section)])
        return (eng_rich if eng_rich is not None else english_abstract, ne_rich if ne_rich is not None else non_english_abstract)

    except Exception as ex:
        error_list.append(f"extract_abstract | {ex}")
        return None, None


def extract_keywords(soup):
    try:
        keywords_div = soup.find("div", {"class": "Keywords"}) or soup.find("div", {"class": "keywords"}) or soup.find("div", {"id": "keywords"})
        if not keywords_div:
            return None

        # Find the section whose <h2> says "Keywords" or "Key words" — skip Abbreviations
        keyword_section = None
        for section in keywords_div.find_all("div", {"class": "keywords-section"}):
            h2 = section.find("h2")
            if h2 and h2.get_text(strip=True).lower() in ("keywords", "key words"):
                keyword_section = section
                break

        if keyword_section is None:
            return None

        # Only grab top-level keyword divs — avoids nested abbreviation expansions
        keywords = []
        for kw in keyword_section.find_all("div", {"class": "keyword"}, recursive=False):
            text = kw.find("span")
            if text:
                keywords.append(text.get_text(strip=True))

        if not keywords:
            target = None
            for section in keywords_div.find_all("div", {"class": "keywords-section"}):
                h2 = section.find("h2")
                if not (h2 and h2.get_text(strip=True).lower() in ("keywords", "key words")):
                    continue
                if section.find_all("div", {"class": "keyword"}, recursive=False):
                    target = section
                    break
                if target is None:
                    target = section
            if target is not None:
                kw_divs = target.find_all("div", {"class": "keyword"}, recursive=False)
                if not kw_divs:
                    kw_divs = target.find_all("div", {"class": "keyword"})
                for kw in kw_divs:
                    span = kw.find("span")
                    if span:
                        txt = span.get_text(" ", strip=True)
                        if txt:
                            keywords.append(txt)

        return "|".join(keywords) if keywords else None

    except Exception as ex:
        error_list.append(f"extract_keywords | {ex}")
        return None


def extract_copyright_year(soup):
    try:
        copyright_line = soup.select_one("div.Copyright span.copyright-line")
        if copyright_line:
            text = copyright_line.get_text(" ", strip=True)
            match = re.search(r"\b(19|20)\d{2}\b", text)
            if match:
                return match.group(0)

        for div in soup.select("div.u-margin-l-ver.text-xs"):
            text = div.get_text(" ", strip=True)
            if "©" not in text and "copyright" not in text.lower():
                continue
            match = re.search(r"©\s*((?:19|20)\d{2})", text)  # year right after ©
            if match:
                return match.group(1)
            match = re.search(r"\b(19|20)\d{2}\b", text)  # any year in the line
            if match:
                return match.group(0)

        return None
    except Exception as ex:
        error_list.append(f"extract_copyright_year | {ex}")
        return None


def extract_copyright(soup):
    try:
        # keep the regex search — used as a guard that returns the full raw URL
        def _license_url(value):
            if not value:
                return None
            m = re.search(
                r"https?://[^\s\"'>]*creativecommons\.org/licenses/[a-z-]+/[\d.]+/?",
                value,
                re.I,
            )
            return m.group(0) if m else None

        # ── TYPE 1: div-based layouts (existing) — return raw href ──
        for selector in ("div.content-meta-license", "div.License"):
            block = soup.select_one(selector)
            if not block:
                continue
            a = block.find("a", href=True)
            if not a:
                continue
            href = a.get("href", "").strip()
            if href:
                return href

        # ── TYPE 2: <meta> tag fallback (new) — regex guard, return raw URL ──
        for meta in soup.find_all("meta"):
            url = _license_url(meta.get("content", ""))
            if url:
                return url

        return None
    except Exception as ex:
        error_list.append(f"extract_license | {ex}")
        return None


def extract_funding_details(soup):
    try:

        def collect(first_div):
            # join the funding div with any consecutive u-margin-s-bottom siblings
            parts, node = [], first_div
            while node is not None:
                if getattr(node, "name", None) == "div" and "u-margin-s-bottom" in (node.get("class") or []):
                    t = re.sub(r"\s+", " ", node.get_text(" ", strip=True)).strip()
                    if t:
                        parts.append(t)
                node = node.find_next_sibling()
            return " ".join(parts)

        # text that means "no funding" -> blank (EN + FR)
        blank_markers = {
            "none",
            "aucun",
            "aucune",
            "n/a",
            "na",
            "no funding sources to report",
            "no funding received",
            "no funding",
            "no funding obtained.",
            "no funding was received for this study",
            "nil",
        }
        # ── existing logic ──
        sections = soup.find_all("h2", class_="section-title")
        for sec in sections:
            if sec.get_text(strip=True).lower() == "funding":
                funding_div = sec.find_next("div", class_="u-margin-s-bottom")
                if funding_div:
                    text = collect(funding_div)
                    if text.rstrip(".").strip().lower() in blank_markers:
                        return ""
                    return text

        # ── fallback layouts (EN "Funding/Funding Sources", FR "Financement") ──
        for heading in soup.find_all(["h2", "h3", "h4"]):
            label = heading.get_text(" ", strip=True).lower().strip()
            if not re.match(r"(funding|financement)", label):
                continue
            funding_div = heading.find_next_sibling("div", class_="u-margin-s-bottom")
            if funding_div is None and heading.parent is not None:
                funding_div = heading.parent.find("div", class_="u-margin-s-bottom")
            if funding_div is None:
                continue
            text = collect(funding_div)
            if not text:
                return ""
            if text.rstrip(".").strip().lower() in blank_markers:
                return ""
            return text

        return ""
    except Exception as ex:
        error_list.append(f"extract_funding_details | {ex}")
        return None


def extract_article_type(article_list_item_soup):
    try:
        type_row = article_list_item_soup.find("span", {"class": "js-article-subtype"})
        if type_row:
            return type_row.get_text(strip=True)

        # Fallback: regex on raw HTML
        html = str(article_list_item_soup)
        match = re.search(r"js-article-subtype[^>]*>(.*?)<", html)
        if match:
            return match.group(1).strip()

        return None
    except Exception as ex:
        error_list.append(f"extract_article_type | {ex}")
        return None


def extract_corporate_author_details(soup):
    try:
        corporate_auth_row = soup.find("div", {"class": "author-collaboration"})
        return (corporate_auth_row.find("div", {"class": "title"})).find("span").text
    except Exception as ex:
        return None


def extract_editor_details(soup):
    def _clean_ws(text):
        return re.sub(r"\s+", " ", text).strip()

    try:
        container = soup.find("div", class_="js-authors") or soup.find("div", {"class": "js-title-editors-group"})
        if container is None:
            return []

        editors = []
        units = container.find_all("li")
        if not units:
            units = container.find_all("span", class_="ag-name-affiliation") or [container]

        for unit in units:
            name_span = unit.find("span", class_="ag-name")
            if name_span is None:
                continue
            name = _clean_ws(name_span.get_text(" ", strip=True))
            if not name:
                continue

            unit_classes = unit.get("class") or []
            if "ag-name-affiliation" in unit_classes:
                anchor = unit
            else:
                anchor = unit.find("span", class_="ag-name-affiliation") or name_span
            aff_span = anchor.find_next_sibling("span")
            aff = _clean_affiliation_text(_clean_ws(aff_span.get_text(" ", strip=True))) if aff_span else ""
            editors.append((name, aff, ""))

        return editors

    except Exception as ex:
        error_list.append(f"extract_editor_details | {ex}")
        return []


# ============================================================================
# PARSERS THAT NEED THE BROWSER (async)
# ============================================================================
def extract_reference_count(soup):
    try:
        # Layout 1: full article — paginatedReferences
        container = soup.find("div", {"class": "paginatedReferences"})
        if container:
            header = container.find("h2")
            if header:
                for span in header.find_all("span"):
                    text = span.get_text(strip=True)
                    if text.startswith("(") and text.endswith(")"):
                        num = text[1:-1]
                        if num.isdigit():
                            return int(num)
            refs = container.find_all("li", {"class": "bib-reference"})
            if refs:
                return len(refs)

        # Layout 3/9/10: preview pages — preview-references
        container = soup.find("div", {"class": "preview-references"})
        if container:
            header = container.find("h2")
            if header:
                for span in header.find_all("span"):
                    text = span.get_text(strip=True)
                    if text.startswith("(") and text.endswith(")"):
                        num = text[1:-1]
                        if num.isdigit():
                            return int(num)
            refs = container.find_all("li", {"class": "bib-reference"})
            if refs:
                return len(refs)

        # Layout 2: full article — bibliography-sec / ol.references
        container = soup.find("ol", {"class": "references"})
        if container:
            refs = container.find_all("li", recursive=False)
            if refs:
                return len(refs)

        return None

    except Exception as ex:
        error_list.append(f"extract_reference_count | {ex}")
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


# ============================================================================
# JSON OUTPUT
# ============================================================================
JSON_FIELDS = [
    ("journal_title", "journal_title"),
    ("journal_url", "journal_url"),
    ("volume", "volume"),
    ("issue", "issue"),
    ("issue_url", "issue_url"),
    ("issue_publication_month", "issue_pub_month"),
    ("issue_publication_day", "issue_pub_day"),
    ("issue_publication_year", "issue_pub_year"),
    ("article_url", "article_url"),
    ("article_publication_month", "article_pub_month"),
    ("article_publication_day", "article_pub_day"),
    ("article_publication_year", "article_pub_year"),
    ("start_page", "start_page"),
    ("last_page", "last_page"),
    ("article_id", "article_id"),
    ("english_title", "english_title"),
    ("foreign_title", "foreign_title"),
    ("doi", "doi"),
    ("english_abstract", "english_abstract"),
    ("non_english_abstract", "non_english_abstract"),
    ("article_type", "article_type"),
    ("author_keyword", "author_keyword"),
    ("reference_count", "reference_count"),
    ("copyright_year", "copyright_year"),
    ("license_type", "copyright"),
    ("funding_status", "funding_details"),
]


class JsonOutput:
    def __init__(self, file_path: str):
        self.articles = []
        self._file = open(file_path, "w", encoding="utf-8", newline="\n")

    def add_format(self, properties):
        # Keep the existing rich-text extractors working without creating a workbook.
        return xlsxwriter.format.Format(properties)

    def close(self):
        if not self._file.closed:
            with self._file:
                json.dump({"articles": self.articles}, self._file, ensure_ascii=False, indent=2)
                self._file.write("\n")


def _json_text(value) -> str:
    if value is None:
        return ""
    if isinstance(value, list):
        # Rich text alternates formatting objects and text fragments.
        return "".join(fragment for fragment in value if isinstance(fragment, str))
    return str(value)


def _json_contacts(details) -> list[dict]:
    if isinstance(details, str):
        details = [[details, "", ""]] if details else []
    contacts = []
    for name, affiliation, email in details or []:
        contact = {
            "name": (name or "").strip(),
            "affiliation": (affiliation or "").strip().replace("/", "\\"),
            "email": (email or "").strip(),
        }
        if any(contact.values()):
            contacts.append(contact)
    return contacts


def create_json_file(title: str):
    try:
        # Defensive: handle None, empty, whitespace-only, illegal chars, long names
        if not title or not str(title).strip():
            safe_title = "untitled"
        else:
            safe_title = str(title).strip()
            # Remove Windows-illegal chars
            safe_title = re.sub(r'[<>:"/\\|?*]', "_", safe_title)
            # Remove control chars (newlines, tabs, etc.)
            safe_title = re.sub(r"[\x00-\x1f\x7f]", "_", safe_title)
            # Collapse runs of underscores/spaces
            safe_title = re.sub(r"[_\s]+", "_", safe_title).strip("_")
            # Truncate to safe length (Windows MAX_PATH considerations)
            safe_title = safe_title[:150] if safe_title else "untitled"
            if not safe_title:
                safe_title = "untitled"

        file_name = f"{safe_title}_{DATE.strftime('%Y%m%d%H%M%S')}.json"
        os.makedirs(folder_path, exist_ok=True)
        file_path = os.path.join(folder_path, file_name)

        # Log what we're about to create — so we can see in errors.txt if it goes wrong
        print(f"  Creating JSON: {file_path}")

        output = JsonOutput(file_path)
        return output, file_path, 0

    except Exception as ex:
        error_list.append(f"create_json_file failed for title={title!r} | {ex}")
        return None, None, 0


def write_into_json(output, article_count: int, record: dict) -> int:
    try:
        article = {key: _json_text(record[source]) for key, source in JSON_FIELDS}
        article["authors"] = _json_contacts(record.get("author_details"))
        article["editors"] = _json_contacts(record.get("editor_details"))
        article["corporate_authors"] = _json_contacts(record.get("corporate_author_details"))
        output.articles.append(article)
        return article_count + 1

    except Exception as ex:
        error_list.append(f"write_into_json | {ex}")
        return article_count


# ============================================================================
# MAIN ASYNC FLOW
# ============================================================================
async def scrape_article(
    browser,  # ← single persistent browser passed from caller
    based_url: str,
    article_soup_node,  # <li> element from issue listing (for article_type)
    main_title: str,
    main_journal_url: str,
    latest_issue_url: str,
    volume,
    issue,
    pub_year,
    pub_month,
    editor_details,
    output,
    article_count: int,
) -> tuple:
    article_link_node = article_soup_node.find(
        "a",
        {"class": "anchor article-content-title u-margin-xs-top u-margin-s-bottom anchor-primary"},
    )
    if not article_link_node:
        return article_count, False

    article_link = article_link_node["href"]
    article_url = f"{based_url}{article_link}"
    english_title = None

    # Article type comes from the issue listing node, not the article page.
    article_type = extract_article_type(article_soup_node)

    try:
        article_soup, article_page, article_html = await fetch_soup_with_page(browser, article_url, wait_id="root")
        if article_soup is None:
            return article_count, False

        # ── Block detection (mirrors check_script.py is_blocked check) ──
        page_title_tag = article_soup.find("title")
        page_title_str = page_title_tag.get_text(strip=True) if page_title_tag else ""
        if is_blocked(page_title_str):
            print(f"  ⚠️  Block detected! Title='{page_title_str}'")
            return article_count, True  # ← signal caller: restart browser, retry

        # --- Article metadata ---
        article_data = extract_article_data(article_soup)
        article_pub_date = article_pub_month = article_pub_year = None
        article_start_page = article_end_page = article_id = None
        if article_data is None:
            _log_missing("article_data", article_url, None, main_title)
        if article_data is not None:
            article_pub_date = article_data["publication_day"]
            article_pub_month = article_data["publication_month"]
            article_pub_year = article_data["publication_year"]
            article_start_page = article_data["start_page"]
            article_end_page = article_data["end_page"]
            article_id = article_data["article_id"]

        # --- Titles (rich text aware) ---
        eng_rich, english_title, for_rich, foreign_title = extract_article_titles(article_soup, output)

        # Diagnostic block when title extraction fails
        if english_title is None:
            _log_missing("english_title", article_url, None, main_title)
            html_str = str(article_soup)
            page_title_tag = article_soup.find("title")
            page_title_text = page_title_tag.get_text(strip=True)[:120] if page_title_tag else "NO <title>"
            h1_any = article_soup.find("h1")
            h1_text = h1_any.get_text(" ", strip=True)[:120] if h1_any else "NO <h1>"
            block_markers = [
                "Access Denied",
                "access denied",
                "Please verify you are a human",
                "Pardon Our Interruption",
                "unusual traffic",
                "captcha",
                "Captcha",
                "CAPTCHA",
                "challenge-platform",
                "cf-challenge",
                "Just a moment",
                "blocked",
                "Rate limit",
                "429",
            ]
            matched = [m for m in block_markers if m in html_str]
            print(f"  [!] No title extracted for: {article_url}")
            print(f"      html_len={len(html_str)} | <title>={page_title_text}")
            print(f"      first h1: {h1_text}")
            if matched:
                print(f"      *** BLOCK MARKERS FOUND: {matched} ***")
            if len(html_str) < 20_000:
                print("      *** Page suspiciously small — likely block/challenge ***")

        print(f"  {english_title}")

        # --- Core fields ---
        doi = extract_doi(article_soup)
        if doi is None:
            _log_missing("doi", article_url, english_title, main_title)
        english_abstract, non_english_abstract = extract_abstract(article_soup, output)
        key_words = extract_keywords(article_soup)

        # --- Newly wired fields (were unused in Document 1) ---
        copyright_year = extract_copyright_year(article_soup)
        copyright = extract_copyright(article_soup)
        funding_details = extract_funding_details(article_soup)

        # --- Browser-dependent fields ---
        reference_count = extract_reference_count(article_soup)
        # extract_author_details now returns [name, aff, email] triples and corporate author details
        author_detail_mapping, arp_corporate_details = await extract_author_details(browser, article_html, article_url, page=article_page)

        # --- Optional fields ---
        corporate_author_details = arp_corporate_details if arp_corporate_details is not None else extract_corporate_author_details(article_soup)

        if english_title == "Are you a robot?" or foreign_title == "Are you a robot?":
            return article_count, True  # ← treat as block, restart browser

        final_record = {
            "journal_title": main_title,
            "journal_url": main_journal_url,
            "volume": volume,
            "issue": issue,
            "issue_url": latest_issue_url,
            "issue_pub_month": pub_month,
            "issue_pub_day": "",
            "issue_pub_year": pub_year,
            "article_url": article_url,
            "article_pub_month": article_pub_month,
            "article_pub_day": article_pub_date,
            "article_pub_year": article_pub_year,
            "start_page": article_start_page,
            "last_page": article_end_page,
            "article_id": article_id,
            "english_title": english_title,
            "english_title_rich": eng_rich,
            "foreign_title": foreign_title,
            "foreign_title_rich": for_rich,
            "doi": doi,
            "english_abstract": english_abstract,
            "non_english_abstract": non_english_abstract,
            "article_type": article_type,  # col 19 — now populated
            "author_keyword": key_words,
            "reference_count": reference_count,
            "copyright_year": copyright_year,  # col 22 — now populated
            "copyright": copyright,  # col 23 — now populated
            "funding_details": funding_details,  # col 24 — now populated
            "author_details": author_detail_mapping,  # now [name, aff, email]
            "corporate_author_details": corporate_author_details,
            "editor_details": editor_details,
        }

        article_count = write_into_json(output, article_count, final_record)

    except Exception as ex:
        err = f"error - {ex} | article link - {article_url} | " f"title - {english_title} | main title {main_title}"
        error_list.append(str(err))

    return article_count, False


def get_total_pages(soup):

    try:
        label = soup.find("span", class_="pagination-pages-label")
        if not label:
            return

        text = label.get_text(strip=True)
        match = re.search(r"Page\s+\d+\s+of\s+(\d+)", text, re.IGNORECASE)
        if match:
            return int(match.group(1))

        return
    except Exception as e:
        error_list.append(f"get_total_pages | {e}")
        return


async def get_all_articles_with_pagination(browser, issue_url, first_soup=None):

    all_articles = []

    try:
        if first_soup is None:
            first_soup = await fetch_soup(browser, issue_url, wait_id="react-root")
        if first_soup is None:
            return []

        total_pages = get_total_pages(first_soup) or 1
        print(f"[Pagination] Total pages: {total_pages}")

        articles = get_all_article_list(first_soup) or []
        if not articles:
            error_list.append(f"PAGINATION PAGE EMPTY | page 1 | {issue_url}")
            print("  ⚠️ Page 1 returned 0 articles — verify against site")
            refetch = await fetch_soup(browser, issue_url, wait_id="article-results-rhs-primary")
            if refetch:
                articles = get_all_article_list(refetch) or []
                if not articles:
                    error_list.append(f"PAGINATION PAGE EMPTY AFTER REFETCH | page 1 | {issue_url}")
        all_articles.extend(articles)

        if total_pages == 1:
            return all_articles

        for page_num in range(2, total_pages + 1):
            page_url = f"{issue_url}?page={page_num}"
            print(f"[Pagination] Fetching page {page_num}: {page_url}")

            soup = None
            for page_attempt in range(1, 4):  # retry up to 3 times per page
                soup = await fetch_soup(browser, page_url, wait_id="react-root")
                if soup is None:
                    await asyncio.sleep(8)  # wait before retrying
                    continue

                title_tag = soup.find("title")
                title_str = title_tag.get_text(strip=True) if title_tag else ""
                if is_blocked(title_str):
                    print(f"  ⚠️ Block detected on page {page_num} attempt {page_attempt}! Title='{title_str}'")
                    soup = None
                    await asyncio.sleep(8)
                    continue
                break

            if soup is None:
                error_list.append(f"PAGINATION PAGE FAILED | page {page_num} | {page_url}")
                print(f"  ❌ Failed to fetch page {page_num} after retries — skipping")
                continue

            articles = get_all_article_list(soup) or []
            if not articles:
                error_list.append(f"PAGINATION PAGE EMPTY | page {page_num} | {page_url}")
                print(f"  ⚠️ Page {page_num} returned 0 articles — verify against site")
            all_articles.extend(articles)

        return all_articles

    except Exception as e:
        error_list.append(f"get_all_articles_with_pagination | {e}")
        return all_articles


async def scrape_journal(url: str):
    setup_browser = None
    main_soup = issue_soup = latest_issue_url = based_url = None

    try:
        setup_browser = await start_browser()

        main_soup = await fetch_soup(setup_browser, url, wait_id="all-issues")
        if main_soup is None:
            return

        parsed = urlparse(url)
        based_url = f"{parsed.scheme}://{parsed.netloc}"
        latest_issue_link = get_latest_issue_url(main_soup)

        if str(latest_issue_link) == "Skip":
            print("Latest issue is In Progress!")
            return
        if latest_issue_link is None:
            return

        latest_issue_url = f"{based_url}{latest_issue_link}"

        # this link is use to check paginations
        # latest_issue_url = "https://www.sciencedirect.com/journal/journal-of-hospitality-and-tourism-management/vol/66/suppl/C"

        issue_soup = await fetch_soup(setup_browser, latest_issue_url, wait_id="react-root")
        if issue_soup is None:
            return

        main_title = extract_main_title(issue_soup)
        print(f"\nScraping started for: [{main_title}]\n")

    finally:
        if setup_browser is not None:
            try:
                await setup_browser.stop()
            except Exception as stop_err:
                error_list.append(f"setup_browser.stop failed | {stop_err}")
            await asyncio.sleep(1.0)

    output, out_json_path, article_count = create_json_file(main_title)
    if output is None:
        print(f"Failed to create JSON for {main_title}")
        return

    try:
        volume, issue, pub_year, pub_month = extract_issue_volume_pubdate(issue_soup, latest_issue_url)

        print(f"Latest issue URL: {latest_issue_url}")
        print(f"Volume: {volume} | Issue: {issue} | Pub Year: {pub_year} | Pub Month: {pub_month}")
        editor_details = extract_editor_details(issue_soup)
        pagination_browser = await start_browser()
        try:
            article_list = await get_all_articles_with_pagination(pagination_browser, latest_issue_url, first_soup=issue_soup)
        finally:
            await pagination_browser.stop()

        if article_list:
            # article_list = article_list[:3]
            total = len(article_list)

            # ── Single persistent browser for ALL articles ──────────────────
            # Pattern mirrors check_script.py: warm-up session first, then
            # loop with while+continue so blocked articles are retried after
            # a browser restart — idx does NOT increment on block.
            article_browser = await start_browser()
            await setup_browser_session(article_browser, latest_issue_url)

            try:
                idx = 0
                block_retries = 0
                timeout_retries = 0
                block_start_time = None
                MAX_BLOCK_RETRIES = 6
                while idx < total:
                    article = article_list[idx]
                    print(f"[{idx + 1}/{total}] ", end="")

                    try:
                        article_count, blocked = await asyncio.wait_for(
                            scrape_article(
                                article_browser,
                                based_url,
                                article,
                                main_title,
                                url,
                                latest_issue_url,
                                volume,
                                issue,
                                pub_year,
                                pub_month,
                                editor_details,
                                output,
                                article_count,
                            ),
                            timeout=ARTICLE_TIMEOUT,
                        )
                    except asyncio.TimeoutError:
                        timeout_retries += 1
                        error_list.append(f"TIMEOUT article {idx + 1} (attempt {timeout_retries}/{MAX_TIMEOUT_RETRIES}) | {latest_issue_url}")
                        print(f"  ⏱ Stuck on article {idx + 1} — timeout ({timeout_retries}/{MAX_TIMEOUT_RETRIES}), restarting browser...")
                        if timeout_retries > MAX_TIMEOUT_RETRIES:
                            error_list.append(f"GAVE UP article {idx + 1} after {MAX_TIMEOUT_RETRIES} timeouts | {latest_issue_url}")
                            print(f"  ✋ Gave up on article {idx + 1} after {MAX_TIMEOUT_RETRIES} timeouts — skipping")
                            idx += 1
                            timeout_retries = 0
                        else:
                            try:
                                await article_browser.stop()
                            except Exception:
                                pass
                            await asyncio.sleep(10)
                            article_browser = await start_browser()
                            await setup_browser_session(article_browser, latest_issue_url)
                            print(f"  🔄 Browser ready — retrying article {idx + 1}...")
                        continue

                    if blocked:
                        block_retries += 1
                        if block_retries == 1:
                            block_start_time = time.monotonic()
                        elapsed = time.monotonic() - block_start_time
                        if block_retries > MAX_BLOCK_RETRIES or elapsed > BLOCK_RECOVERY_TIMEOUT:
                            reason = f"block timeout ({elapsed:.0f}s)" if elapsed > BLOCK_RECOVERY_TIMEOUT else f"{MAX_BLOCK_RETRIES} block retries"
                            error_list.append(f"giving up on article {idx + 1} after {reason} | {article}")
                            print(f"  ✋ Gave up on article {idx + 1} — {reason}")
                            idx += 1
                            block_retries = 0
                            block_start_time = None
                            continue

                        print(f"  🔁 Block on article {idx + 1} (retry {block_retries}/{MAX_BLOCK_RETRIES}, {elapsed:.0f}s elapsed) — restarting browser...")
                        try:
                            await article_browser.stop()
                        except Exception:
                            pass
                        await asyncio.sleep(10)
                        article_browser = await start_browser()
                        await setup_browser_session(article_browser, latest_issue_url)
                        print(f"  🔄 Browser ready — retrying article {idx + 1}...")
                        continue

                    block_retries = 0
                    block_start_time = None
                    timeout_retries = 0
                    idx += 1
                    if idx < total:
                        delay = random.uniform(*DELAY_BETWEEN_ARTICLES)
                        await asyncio.sleep(delay)

            finally:
                try:
                    await article_browser.stop()
                except Exception:
                    pass
    finally:
        output.close()
        print(f"JSON saved: {out_json_path}")


async def main_async():
    try:
        with open("urlDetails.txt", "r", encoding="utf-8") as file:
            url_list = file.read().strip().splitlines()
    except Exception as error:
        error_list.append(str(error))
        url_list = []

    if not url_list:
        print("No URLs found in urlDetails.txt")
        return

    for url_data in url_list:
        try:
            url = url_data.strip()
            await scrape_journal(url.strip())
        except Exception as e:
            print(f"Journal-level error: {e}")
            error_list.append(str(e))
            continue


def main():
    # Check the required parser before opening browsers or retrying every URL.
    try:
        BeautifulSoup("<html></html>", "lxml")
    except FeatureNotFound as exc:
        message = (
            f"Required HTML parser unavailable: {exc}\n"
            f'Install it in this Python environment with: & "{sys.executable}" -m pip install lxml'
        )
        error_list.append(message)
        raise SystemExit(message) from None
    asyncio.run(main_async())


if __name__ == "__main__":
    try:
        main()
    finally:
        try:
            os.makedirs(folder_path, exist_ok=True)
            with open(os.path.join(folder_path, "errors.txt"), "w", encoding="utf-8") as f:
                for err in error_list:
                    f.write(f"{err}\n")
        except Exception as e:
            print(f"Could not write errors.txt: {e}")
