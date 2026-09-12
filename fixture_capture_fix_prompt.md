# Task: fix fixture capture (Cloudflare) and fix the selection catalogue

The previous round produced correct tooling against a faulty specification. Two
defects, both mine, plus one open question the fixtures must answer.

---

## Defect 1 — the capture warms on the wrong URL

`scripts/capture_fixtures.py` currently calls:

```python
await legacy.setup_browser_session(browser, fixtures[0].article_url)
```

`CABIACQ.py` never seeds a session with an article URL. At line 1846, and again
after every browser restart at 1893 and 1918, it seeds with the **issue listing
URL**:

```python
await setup_browser_session(article_browser, latest_issue_url)
```

A cold browser landing directly on `/science/article/pii/...` is a much more
aggressive first request than one landing on a listing page, and ScienceDirect
protects article pages more heavily.

This compounds. `setup_browser_session` swallows its own failures — the CF solve
is wrapped in `except Exception: pass` and a failed warm-up only prints a
warning. So a blocked warm-up returns normally, reports "Session ready", and
every subsequent article fetch is blocked. That is why both attempts failed
identically with no useful diagnostic.

### Required changes

**Seed from the issue URL.** The golden JSON carries `issue_url` as one of its
29 keys. Add `issue_url` to each record in `SELECTION.json` (regenerate it), and
carry it on the fixture dataclass.

**Group by issue and warm once per group.** Restructure the run:

```
for issue_url, fixtures_in_issue in grouped_by_issue:
    browser = await start_browser()
    await setup_browser_session(browser, issue_url)
    verify_session_cleared(browser)        # see below — raises on failure
    for fixture in fixtures_in_issue:
        fetch, save, delay
    close browser
```

This mirrors the legacy access pattern: one warmed session per issue, articles
fetched from within it.

**Verify the warm-up instead of trusting it.** After `setup_browser_session`,
read `document.title` and check it against `legacy.is_blocked`. If still blocked,
retry the warm-up once after the legacy restart delay. If it fails again, raise
with a clear message naming the issue URL and stop the whole run. Do not attempt
any article fetch on an unverified session — eight guaranteed failures teach
nothing and look like eight separate problems.

**Raise the per-article block allowance** from 2 to 4 attempts, with the legacy
restart delay between them, and re-warm on the issue URL after each restart.

**For `EXTRAS.txt` entries**, allow an optional second URL on the line giving the
issue or journal page to warm from. If absent, derive it: strip back to
`/journal/<slug>/` or `/bookseries/<slug>/` and append `issues` or `volumes`.
Update the `EXTRAS.txt` template comment to document this.

---

## Defect 2 — the edge-case catalogue is wrong

Measured across all 54 articles in the three golden files:

```
author_email             53      many_authors             19
empty_abstract            6      everything else           0
```

`author_email` is present on 98% of articles, so it is not an edge case. And
because `ordinary` was defined as "none of the above", `author_email` alone made
`ordinary` unreachable, which is why the top-up to eight never ran. The selector
implemented the spec correctly; the spec was wrong.

### Required changes

**Drop `author_email` from the catalogue entirely.** Replace with
`missing_email` (any author with an empty `email`), which is the informative
case.

**Redefine `ordinary`**: non-empty `english_abstract`, 2–5 authors, non-empty
`start_page`, non-empty `doi`. Stop making it conditional on other keys.

**Fix the top-up.** After greedy set cover, keep adding articles until 8 are
selected, preferring, in order: articles from a journal not yet represented; a
different `article_type` than those already chosen; then any remaining article.
Never stop below 8 unless fewer than 8 articles exist in total.

**Record coverage honestly.** `uncovered_keys` should list only keys no article
in the corpus satisfies, which is the operator's signal for what to add to
`EXTRAS.txt`. Print the per-key counts from the corpus alongside it so the
operator can see whether a zero means "rare" or "never extracted".

---

## Open question the fixtures must answer

`corporate_authors` and `editors` are empty in **all 54 articles across three
journals**. Two possible explanations, and nothing in the current data
distinguishes them:

1. These journals genuinely publish no collaboration authors and no editors.
2. `_collect_collaborations` and the editor extraction are broken and have been
   returning empty lists all along.

If it is the second, gate 4 would ship both functions with zero coverage.

Add a section to `SOURCE.md` titled **Open question: corporate_authors and
editors**, stating the counts, both hypotheses, and that the bookseries fixture
is expected to resolve the editor case, since book content carries editors where
journal articles do not. Gate 4's parse tests must assert against whichever
answer the fixtures produce, not assume either.

---

## Constraints

- `CABIACQ.py` unchanged. `git diff CABIACQ.py` empty at the end.
- No changes under `publisher_lakehouse/`.
- Do not write `tests/test_sciencedirect_parse.py`. That is gate 4.
- `urlDetails.txt` is restored to 85 URLs by the operator before you start; do
  not modify it.

## Acceptance

- `python scripts/select_fixtures.py` selects **8** fixtures, spread across all
  three journals, prints per-key corpus counts, and lists only genuinely
  unreachable keys as uncovered.
- `python scripts/capture_fixtures.py` warms per issue, verifies each warm-up,
  and saves HTML for every selected fixture. A blocked warm-up aborts the run
  with the issue URL named.
- `SOURCE.md` records every fixture, its coverage, the rich-text findings, and
  the open question above.
- Full suite green on Windows and Linux.

Stop after this. Report how many fixtures captured successfully, and whether any
of them contain `<sub>`, `<sup>`, `<i>` or `<em>` inside the title element.
