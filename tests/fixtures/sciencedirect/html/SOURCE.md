# ScienceDirect HTML fixtures

Captured 2026-09-12T17:28:02.418487Z from the article URLs in the golden JSON files
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
| richtext_subscript_title.html | S1684118225002166 | Journal of Microbiology, Immunology and Infection | richtext_subscript_title | title: <sub>, <em> |

## Issue pages

| Slug | Issue URL | Page title | Byte size |
|---|---|---|---:|
| journal_journal_of_microbiology_immunology_and_infection_vol_59_issue_4 | https://www.sciencedirect.com/journal/journal-of-microbiology-immunology-and-infection/vol/59/issue/4 | Journal of Microbiology, Immunology and Infection \| Vol 59, Issue 4, Pages 425-530 (August 2026) \| ScienceDirect.com by Elsevier | 420887 |
| journal_indian_journal_of_tuberculosis_vol_73_issue_3 | https://www.sciencedirect.com/journal/indian-journal-of-tuberculosis/vol/73/issue/3 | Indian Journal of Tuberculosis \| Vol 73, Issue 3, Pages 289-424 (July 2026) \| ScienceDirect.com by Elsevier | 397657 |
| journal_international_journal_of_sediment_research_vol_41_issue_5 | https://www.sciencedirect.com/journal/international-journal-of-sediment-research/vol/41/issue/5 | International Journal of Sediment Research \| Vol 41, Issue 5, Pages 727-908 (October 2026) \| ScienceDirect.com by Elsevier | 403812 |
| journal_revue_veterinaire_clinique_vol_61_issue_2 | https://www.sciencedirect.com/journal/revue-veterinaire-clinique/vol/61/issue/2 | Revue Vétérinaire Clinique \| Vol 61, Issue 2, Pages 65-118 (May 2026) \| ScienceDirect.com by Elsevier | 377983 |
| bookseries_advances_in_parasitology_vol_131_suppl_c | https://www.sciencedirect.com/bookseries/advances-in-parasitology/vol/131/suppl/C | Advances in Parasitology \| Volume 131: Advances in Parasitology \| ScienceDirect.com by Elsevier | 359515 |
| journal_the_lancet_global_health_vol_14_issue_6 | https://www.sciencedirect.com/journal/the-lancet-global-health/vol/14/issue/6 | The Lancet Global Health \| Vol 14, Issue 6, June 2026 \| ScienceDirect.com by Elsevier | 429613 |
| journal_pedosphere_vol_36_issue_3 | https://www.sciencedirect.com/journal/pedosphere/vol/36/issue/3 | Pedosphere \| Vol 36, Issue 3, Pages 669-830 (June 2026) \| ScienceDirect.com by Elsevier | 368986 |
| journal_journal_of_microbiology_immunology_and_infection_vol_59_issue_3 | https://www.sciencedirect.com/journal/journal-of-microbiology-immunology-and-infection/vol/59/issue/3 | Journal of Microbiology, Immunology and Infection \| Vol 59, Issue 3, Pages 285-424 (June 2026) \| ScienceDirect.com by Elsevier | 417415 |

## Open question: corporate_authors and editors

Across all 54 articles in the three golden files, `corporate_authors` is non-empty for 0 articles and `editors` is non-empty for 0 articles.

Two hypotheses remain:

1. These journals genuinely publish no collaboration authors and no editors.
2. `_collect_collaborations` and editor extraction have been returning empty lists incorrectly.

The bookseries fixture is expected to resolve the editor case because book content carries editors where journal articles do not.

Gate 4 parse tests must assert whichever answer the captured fixtures establish, rather than assuming either hypothesis.
