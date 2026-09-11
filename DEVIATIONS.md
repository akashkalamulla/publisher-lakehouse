# Phase 1 deviations from `CABIACQ.py`

This file is cumulative and will be updated at each implementation review
gate.

## Implemented at review gate 1

- Configuration and the run timestamp are no longer evaluated while common
  modules are imported. Settings are loaded explicitly at the application
  boundary. This removes the legacy import-time `config.ini` read and frozen
  module-level `DATE` while leaving the legacy script untouched for regression
  comparison.
- The operator config retains this repository's existing
  `C:\CABIACQ_NEW1\` output location. The `D:\new_test1\` value in the brief is
  treated as an illustrative example, not as an instruction to redirect the
  user's existing output.
- Rich-text conversion accepts standalone unformatted strings between
  XlsxWriter format/text pairs. The legacy abstract builder produces this
  hybrid shape; retaining it is necessary to preserve formatting while
  `flatten()` remains identical to the old `_json_text()` behavior.

No extraction or browser behavior has changed at this gate.

