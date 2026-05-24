# Iteration 01 — Initial audit

**Date:** 2026-05-24
**Trigger:** user request — _«провести архитектурный аудит текущего проекта»_

## Input context from user

- Project is a trading robot meant to autonomously act on the market for profit.
- No prior context provided.

## Done by agent

1. Walked the whole `src/` tree (≈ 130 files).
2. Read every entry-point, every factory, every interface, sampled implementations across all layers (Predictor, Strategy, Assessor, Executor, DataSource, ContextProvider, Market, Portfolio, Logger).
3. Reconstructed an architecture map → [`../architecture-overview.md`](../architecture-overview.md).
4. Catalogued 51 findings in [`../findings.md`](../findings.md):
   - 🔴 8 critical, 🟠 12 high, 🟡 14 medium, 🟢 6 low, 🔵 11 questions.
5. Created the `context/` folder structure and visual report [`../report.html`](../report.html).

## Open questions waiting for user

See `Q-01` … `Q-11` in [`../findings.md`](../findings.md#-open-questions-to-the-user).

## Highest-priority asks

1. **RUN-01** — does the project actually start on your machine? If yes, how? The `src/Contracts/Predictor/RandomForestPredictor/` folder has no `__init__.py` and `Contracts/__init__.py` imports through it.
2. **Q-02** — is `main.py` (ML grid-search) the current focus, or is the trading core (Factory / Executor / Market) being actively built?
3. **TRADE-01 / Q-03** — which broker API will execute orders? This dictates much of the next layer of design.

## Status snapshot at end of iteration

All 51 items are 🟥 `open`. No fixes applied yet.
