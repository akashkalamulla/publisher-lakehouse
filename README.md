# Publisher Lakehouse

Publisher Lakehouse is being built as a deduplicated academic-metadata
ingestion platform. ScienceDirect is the Phase 1 pilot; Springer and later
lakehouse layers remain out of scope for this phase.

## Implementation status

The project follows the review gates in
`claude_code_prompt_phase1_sciencedirect.md`. Review gate 1 is complete:

- packaging and separated environment/operator settings;
- publisher-aware URL normalization and stable URL hashes;
- canonical payload hashes;
- legacy-compatible run IDs;
- neutral rich-text segments; and
- structured JSON logging with run-scoped error collection.

The ingestion, manifest, browser, export, and CLI commands are intentionally
not available until their later review gates are approved.

## Configuration

Copy `.env.example` to `.env` for environment-specific settings. Operator
behavior lives separately in `config/config.ini`. Importing parse or common
modules does not read either file or create output directories.

## Tests

Install the project in its virtual environment and run the offline unit suite:

```powershell
& .\.venv\Scripts\python.exe -m pip install -e '.[test]'
& .\.venv\Scripts\python.exe -m pytest
```

The README will be expanded with CLI usage, counter definitions, and the
three-run acceptance procedure when those components exist.

