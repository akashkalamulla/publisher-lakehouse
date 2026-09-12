error_list: list[str] = []


def _log_missing(field: str, article_url: str, title, journal) -> None:
    msg = f"[MISSING] {field} | url: {article_url} | " f"title: {title or 'N/A'} | journal: {journal or 'N/A'}"
    error_list.append(msg)
    print(f"  ⚠️  MISSING {field} — {article_url}")
