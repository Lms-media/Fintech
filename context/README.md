# Context Folder — Architectural Audit

Single source of truth for the iterative architectural audit of the trading-robot project.

## Structure

| File                                                   | Purpose                                                                         |
| ------------------------------------------------------ | ------------------------------------------------------------------------------- |
| [`README.md`](README.md)                               | This file. Folder map and conventions.                                          |
| [`architecture-overview.md`](architecture-overview.md) | Reconstructed architectural map of the project (layers, data flow, components). |
| [`findings.md`](findings.md)                           | **Master list** of problems / questions / observations. Status is tracked here. |
| [`report.html`](report.html)                           | Visual rendering of `findings.md`, grouped by category and severity.            |
| [`iterations/`](iterations/)                           | Per-iteration notes: what was asked, what the user answered, what changed.      |
| [`glossary.md`](glossary.md)                           | Domain terms / accumulated project knowledge from user answers.                 |

## Conventions

### Finding ID

`<CATEGORY>-<NN>` — e.g. `ARCH-01`, `RUN-03`, `ML-07`. IDs are **stable**: once assigned, they don't move even if items are reordered or split.

### Status values

| Status           | Meaning                                            |
| ---------------- | -------------------------------------------------- |
| 🟥 `open`        | New, not yet discussed.                            |
| 🟧 `in-progress` | User has acknowledged / partial info given.        |
| 🟦 `answered`    | Question answered (informational items).           |
| 🟩 `resolved`    | Problem fixed in code.                             |
| ⬛ `wontfix`     | User explicitly declined (with reason recorded).   |
| 🟪 `superseded`  | Replaced by another finding (link to replacement). |
| ⬜ `obsolete`    | No longer relevant (with reason).                  |

**Items are never deleted**, only restatused, per user's instruction.

### Severity / Priority

| Symbol | Level      | Meaning                                              |
| ------ | ---------- | ---------------------------------------------------- |
| 🔴     | `critical` | Blocks running / correctness / data loss / security. |
| 🟠     | `high`     | Significant design flaw, bug-prone, hard to extend.  |
| 🟡     | `medium`   | Code smell, maintainability, minor bugs.             |
| 🟢     | `low`      | Style, naming, polish, nice-to-have.                 |
| 🔵     | `question` | Not a problem — clarification request.               |

### Categories

- `ARCH` — Architecture, layering, dependencies
- `RUN` — Runtime / does-it-even-start
- `DOM` — Domain modelling, contracts, value objects
- `ML` — ML / predictors / training pipeline
- `TRADE`— Trading logic correctness (execution, portfolio, market)
- `CONC` — Concurrency, async, threading
- `IO` — I/O, persistence, network, logging
- `TEST` — Testing, CI, reproducibility
- `SEC` — Security, credentials, secrets
- `DX` — Developer experience, packaging, tooling
- `Q` — Open questions to the user

## Workflow

1. Agent reads `findings.md` to know current state.
2. User provides answers / makes fixes.
3. Agent updates statuses (never deletes), adds new items if discovered, regenerates `report.html`.
4. New iteration notes appended to `iterations/`.
