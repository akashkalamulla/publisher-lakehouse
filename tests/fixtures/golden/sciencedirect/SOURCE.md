# ScienceDirect golden fixture provenance

| File | Journal | Journal URL | Issue URL | Articles |
| --- | --- | --- | --- | ---: |
| `Indian_Journal_of_Tuberculosis_20260911102611.json` | Indian Journal of Tuberculosis | https://www.sciencedirect.com/journal/indian-journal-of-tuberculosis/issues | https://www.sciencedirect.com/journal/indian-journal-of-tuberculosis/vol/73/issue/3 | 24 |
| `International_Journal_of_Sediment_Research_20260911102611.json` | International Journal of Sediment Research | https://www.sciencedirect.com/journal/international-journal-of-sediment-research/issues | https://www.sciencedirect.com/journal/international-journal-of-sediment-research/vol/41/issue/5 | 13 |
| `Journal_of_Microbiology,_Immunology_and_Infection_20260911102611.json` | Journal of Microbiology, Immunology and Infection | https://www.sciencedirect.com/journal/journal-of-microbiology-immunology-and-infection/issues | https://www.sciencedirect.com/journal/journal-of-microbiology-immunology-and-infection/vol/59/issue/4 | 17 |

Total: 54 articles.

These fixtures were produced by running the pre-refactor `CABIACQ.py` on
2026-09-11 (filename stamp `20260911102611`); the run itself was executed on
2026-09-12.

Each file is a single JSON object with one key, `articles`, holding a list of
article objects with 29 fields. Each file covers exactly one issue of one
journal.

These files are the regression baseline for gate 4 (parse) and gate 6 (export).
They are frozen: if a future change makes a test fail against them, the change
is wrong until proven otherwise, and the golden file is not edited to make a
test pass.
