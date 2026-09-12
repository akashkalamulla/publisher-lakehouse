# ScienceDirect HTML fixtures

Captured 2026-09-12T16:14:57.269941Z from the article URLs in the golden JSON files
(`tests/fixtures/golden/sciencedirect/`) plus manual extras.

Regenerate with:

    python scripts/select_fixtures.py
    python scripts/capture_fixtures.py

| Fixture | PII | Journal | Covers | Rich text |
|---|---|---|---|---|
| empty_abstract.html | S1684118226000228 | Journal of Microbiology, Immunology and Infection | empty_abstract, many_authors, missing_email | title: <em> |
| missing_email.html | S0019570725001799 | Indian Journal of Tuberculosis | missing_email, ordinary | none |
| empty_abstract_02.html | S1001627926000612 | International Journal of Sediment Research | empty_abstract | none |
| empty_abstract_03.html | S0019570726001617 | Indian Journal of Tuberculosis | empty_abstract | none |
| empty_abstract_04.html | S0019570726001629 | Indian Journal of Tuberculosis | empty_abstract, missing_email | none |
| many_authors.html | S0019570725001842 | Indian Journal of Tuberculosis | many_authors, missing_email | none |
| missing_email_02.html | S0019570726000879 | Indian Journal of Tuberculosis | missing_email, ordinary | none |
| empty_abstract_05.html | S1684118226000678 | Journal of Microbiology, Immunology and Infection | empty_abstract, many_authors, missing_email | none |
| foreign_title_and_abstract.html | S2214567225000997 | Revue Vétérinaire Clinique | foreign_title_and_abstract | none |
| bookseries_chapter_editor.html | S0065308X26000023 | Advances in Parasitology | bookseries_chapter_editor | none |
| article_id_no_pages.html | S2214109X26001361 | The Lancet Global Health | article_id_no_pages | none |
| richtext_sub_and_superscript.html | S1002016025000335 | Pedosphere | richtext_sub_and_superscript | abstract: <sub>, <sup>, <em> |

## Open question: corporate_authors and editors

Across all 54 articles in the three golden files, `corporate_authors` is non-empty for 0 articles and `editors` is non-empty for 0 articles.

Two hypotheses remain:

1. These journals genuinely publish no collaboration authors and no editors.
2. `_collect_collaborations` and editor extraction have been returning empty lists incorrectly.

The bookseries fixture is expected to resolve the editor case because book content carries editors where journal articles do not.

Gate 4 parse tests must assert whichever answer the captured fixtures establish, rather than assuming either hypothesis.
