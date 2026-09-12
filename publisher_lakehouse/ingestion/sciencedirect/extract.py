import base64
import json
import re
from urllib.parse import unquote

from bs4 import NavigableString

from publisher_lakehouse.ingestion.errors import error_list


skip_issues = "In progress"


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
