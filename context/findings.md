# Findings — Master List

> Iteration: **2** · Last updated: 2026-05-24 · Status legend & conventions: see [`README.md`](README.md).

## Summary

| Severity                   | Iter‑1 | Iter‑2 final | Note                                                               |
| -------------------------- | ------ | ------------ | ------------------------------------------------------------------ |
| 🔴 critical                | 8      | **0**        | 6 🟩 archived · 3 ⬇ demoted to 🟠 (per user: out of diploma scope) |
| 🟠 high                    | 12     | 16           | +3 demoted from 🔴 · +ARCH-08 · +DOM-09                            |
| 🟡 medium                  | 14     | 16           | +ML-07 · +IO-05                                                    |
| 🟢 low                     | 6      | 7            | –                                                                  |
| 🔵 question                | 11     | 11 (all 🟦)  | –                                                                  |
| **Resolved archive**       |        | **6**        | RUN-01, RUN-02, RUN-03, RUN-04, TRADE-02, ML-06                    |
| **Total** (active+archive) | **51** | **55**       | +4 new findings                                                    |

**Iter-2 scope decision (user, 2026-05-24):** "Move resolved items to the end. Everything left in Critical can be either demoted to another priority or live without it — it's not required for my diploma."

Therefore the 3 open-Critical items (RUN-05, TRADE-01, TRADE-03) were demoted to 🟠 High with the explicit ⬇ marker. They remain tracked, just not as diploma blockers.

---

## 🔴 Critical

> **Empty after iter-2.** All 8 critical items closed or demoted. Resolved items live in [Resolved archive](#resolved-archive) at the bottom of this file. Demoted items are now in 🟠 High with the ⬇ marker.

---

## 📦 Demoted-from-Critical (now 🟠 High, ⬇ markers)

> These were `🔴` in iter-1, kept open in iter-2 as `🟦 answered`, and per user's iter-2 decision are demoted to `🟠 High` because they are **not blockers for the diploma scope** (the system can ship without them for the diploma's purposes). They remain tracked.

### RUN-05 ⬇ · was 🔴 → 🟠 · 🟦 answered · `MockContextProvider` returns elapsed-time instead of Unix timestamp

**Why it was critical (kept on file for reference):**

1. **Type lie:** [`IExecutionContext.getTimestamp() -> int`](../src/Interfaces/ValueObjects/IExecutionContext.py:1) is declared `int` (Unix seconds). [`MockContextProvider`](../src/Contracts/ContextProvider/MockContextProvider/MockContextProvider.py:11) stores `time.time()` (float) and emits `time.time() - self._startTimestamp` — a _float, elapsed-since-startup_.
2. **Trigger misfire:** [`TurnBackAction`](../src/Entities/Action/TurnBackAction.py:9) computes `triggerTimestamp = context.getTimestamp() + duration`. With elapsed-time semantics the action fires almost immediately.
3. **Mock ≠ real:** any future broker-backed `IContextProvider` will return Unix seconds. The Strategy/Assessor code consuming `getTimestamp()` is supposed to be broker-agnostic, but currently the two implementations behave incompatibly.
4. **Collides with the planned backtest layer (per Q-08 / ARCH-08):** backtest needs a _virtual_ clock that the data source advances. Current Mock is neither real nor virtual.

**Why demoted (iter-2):** since the diploma's primary deliverable is the _predictor research_ (per Q-11) and the trading pipeline is "moc Strategy/Assessor with a working костяк" (per Q-01), end-to-end timestamp correctness is not on the critical path for the defense. It will be addressed naturally when ARCH-08 lands.

**Recommended fix when scheduled:**

- Short: `MockContextProvider` returns `int(time.time())`. Drop `_startTimestamp`.
- Better: introduce `IClock` abstraction (ARCH-08).

### TRADE-01 ⬇ · was 🔴 → 🟠 · 🟦 answered · No real broker integration

**Original critical because:** the product description says "автономный торговый робот", but no `IMarket` implementation talks to a real exchange.

**Why demoted (iter-2):** per user — "пока не планируем, робот в тестовой среде. Интерфейсы должны позволять без дублирования кода переключиться на реального брокера." Diploma evaluates research + architecture, not a live trading proof.

**Contract for the future swap-in (to keep the architecture chapter defensible):**

- `IMarket.execute(task)` remains broker-agnostic.
- `IExecutionContext` must grow to expose bid/ask, balance, order status (DOM-01).
- `IOrderState` (accepted / partial / filled / rejected / cancelled) — see TRADE-03.

### TRADE-03 ⬇ · was 🔴 → 🟠 · 🟦 answered · No order state machine

Same story as TRADE-01 — needs `IOrderState`, but only becomes urgent when broker integration starts. Will materialise inside ARCH-08.

---

## 🟠 High

### ARCH-01 · 🟠 · 🟦 answered (iter-2) · No composition root for live trading

> **User note (iter-2 via Q-01):** "ты правильно понял, что ещё не всё реализовано. Predictor — реальные алгоритмы; Strategy / Assessor — моковые; провайдеры рынка — моковые; костяк должен работать end-to-end."

**Status:** intentional, scope-driven. Re-priority: the missing piece is not "trading is unwired" but "no second `main_trade.py` that wires Factory→Executor and runs the bot end-to-end as a smoke test". Recommend adding a second entry point that wires `DummyFactory` and runs `SingleActionUseCase` once, even with mock predictor — to **prove the costяк works** as the user described.

Kept 🟦 answered, follow-up captured as DX-06 (entry-point per use-case).

---

### ARCH-02 · 🟠 · 🟥 open · `sys.path.insert(0, …/src)` is the only thing making imports work

No setup.py / pyproject. Recommend either (a) adopt `src/` layout with `pyproject.toml` and `pip install -e .`, or (b) commit to current `sys.path` model and at least document it in README. Choice tied to deployment story (Q-01 says daemon — favours option a).

---

### ARCH-03 · 🟠 · 🟥 open · Interface inheritance with concrete-class shadowing causes Liskov risks

`IValueObject[T]` generic suggests structural equality across implementations, but concrete classes `isinstance(other, Asset)` etc. block it. With the QUIK swap-in goal (TRADE-01 answer), two `IAsset` impls _will_ coexist — current `__eq__` will say they aren't equal even when they refer to the same ticker.

---

### ARCH-04 · 🟠 · 🟪 superseded by ARCH-08 (iter-2)

Original: "no clear separation between simulation and production market". Replaced by the new, more concrete ARCH-08 (introduce `IBacktestClock` + factory split + scenario boundary).

---

### ARCH-05 · 🟠 · 🟥 open · `Factories` is a hand-wired DI container

`MainFactory.__init__` is ~50 lines of constructor wiring; new variations require new factory classes. Given the team is 3 people, and at least one entry-point per _use-case_ (training / testing / backtest / live) is coming, a config-driven factory (e.g. a small `Container` with `register(name, lambda: …)` and parameterized lookup) would pay back in 2–3 iterations.

---

### DOM-01 · 🟠 · 🟦 answered (iter-2) · `IContextProvider` too thin for autonomous trading

Per TRADE-01 answer the contract must grow to support broker swap-in. Concrete shopping list to gate the QUIK migration:

- `IExecutionContext.getBidAsk(assetPair) -> tuple[float, float] | None`
- `IExecutionContext.getOrderbookDepth(assetPair, levels=5)` (optional, may be `None`)
- `IExecutionContext.getAccountBalance() -> dict[IAsset, float] | None` (broker-reported truth)
- `IContextProvider.subscribe(assetPair, callback)` (push model) — optional but needed for realtime per Q-01

Status 🟦, prioritised behind backtest skeleton (Q-08).

---

### DOM-02 · 🟠 · 🟦 answered (iter-2) · `IStrategy` has no access to portfolio / risk state

Per Q-03 answer: **risk lives in Assessor**, not Strategy. So `IStrategy` legitimately does not need portfolio access — strategy emits a _direction + magnitude_ signal; assessor sizes it against the portfolio and applies risk caps. This is consistent with current code (`HalfInAssessor` does sizing).

Re-classified: **not a bug, a documented design decision**. New DOM-09 captures the corollary: Assessor must own _risk caps_, not just sizing.

Kept 🟦 answered; severity could drop to 🟢 once DOM-09 lands.

---

### DOM-03 · 🟠 · 🟦 answered (iter-2) · Strategy / Assessor responsibility overlap

Per Q-03: Strategy = direction+volume from prediction; Assessor = risk + portfolio-aware sizing + action assembly. With this guideline, current `HalfInAssessor` is _correctly_ assessor-shaped: it does sizing and produces a `TurnBackAction`. The remaining smell is that `firstBuy = signal.getDirection() == Up` is directional logic — but it's really _action-direction_ derived from the _strategy-direction_, which is fine.

Status 🟦. Optional follow-up: document this boundary in [`architecture-overview.md`](architecture-overview.md:1).

---

### DOM-04 · 🟠 · 🟥 open · `IPredictor.getCandlesCount()` mixes "model input size" and "dataset window size"

Still open. Recommend split into `getRequiredInputCount()` (predict) and `getDatasetItemCount()` (train), the latter = former + label horizon.

---

### ML-01 · 🟠 · 🟦 answered (iter-2) · Models are not persisted

Per Q-07: persistence is desired but low priority for this iteration. Status kept open but de-prioritised. Captured as ML-07 (iter-2 new finding) with explicit _priority: low, status: deferred_.

---

### ML-02 · 🟠 · 🟥 open · Normalization constants leak future information across train/test

Still open. Fix: re-fit `_initNormalization` on the rolling training window, not once-and-frozen. Also guard against zero variance (`ZeroDivisionError` risk).

---

### ML-03 · 🟠 · 🟥 open · `PredictorRetrainUseCase` retrains on test set (look-ahead bias)

Still open and **most important for diploma's research integrity**. Per Q-11 the diploma includes research results — these results are currently biased. Recommend either:

(a) split retraining into a _real_ rolling-window CV that retrains on `[t-N…t]` and evaluates on `[t+1…t+H]` without ever pushing the evaluated point back into training;

(b) frame the current code as **online learning with concept drift** — _not_ a generalisation benchmark — and report it as such in the diploma.

Either path is fine, but the current report mixes them, which is a defensibility risk.

---

## 🟡 Medium

### ARCH-06 · 🟡 · 🟥 open · `Contracts/` vs. `Services/` boundary unclear

Still open. Recommendation: `Services/` = stateful singletons owned by the application (Logger / Portfolio / Market — global truth), `Contracts/` = pluggable strategies of the trading domain (DataSource / Predictor / Strategy / Assessor / Executor / ContextProvider). Document in [`architecture-overview.md`](architecture-overview.md:1).

---

### ARCH-07 · 🟡 · 🟥 open · Heavy circular import potential

Still open.

---

### DOM-05 · 🟡 · 🟥 open · `Candle` validation duplicated in every `with*` method

Still open.

---

### DOM-06 · 🟡 · 🟥 open · `Candle._vloume` typo in class annotation

Still open. Trivial fix.

---

### DOM-07 · 🟡 · 🟥 open · `Asset.__hash__` uses `(tickerCode, lotSize)` — two assets with same ticker, different lotSize are not equal

Still open. Per Q-06 (lotSize semantics confirmed), the hash is acceptable for now, but worth a docstring.

---

### DOM-08 · 🟡 · 🟥 open · `TurnBackAction._triggerTimestamp: int` consumes a float from MockContext

Tied to RUN-05. Will resolve together with the clock abstraction.

---

### ML-04 · 🟡 · 🟥 open · `keras` is used directly, not `tensorflow.keras` — fragile across versions

Pinned now in requirements (RUN-03 fix), but consistent usage of `from tensorflow import keras` is still recommended.

---

### ML-05 · 🟡 · 🟥 open · `RandomForestPredictor` actually uses XGBoost

Naming bug, easy fix: rename to `GBRegressorPredictor` or document.

---

### ML-06 · 🟡 · 🟩 resolved (iter-2) · `RandomForestPredictor` `__init__.py` missing — same as RUN-01

Closed as duplicate of RUN-01 (resolved). ✅

---

### TRADE-04 · 🟡 · 🟥 open · No fees / slippage / spread modelling

Tied to backtest skeleton (ARCH-08). Once backtest layer exists, add a `FeeModel` to `PortfolioSyncMarket` (BPS commission + bid/ask spread + slippage % per lot).

---

### TRADE-05 · 🟡 · 🟦 answered (iter-2) · Short positions allowed silently in `RuntimePortfolio.sellAsset`

Per Q-07: short-selling is not a planned feature ("планируем только валюты, даже не задумывались о другом"). Recommendation: raise `ValueError` in `sellAsset` when result would go negative, OR add an explicit `allowShort: bool = False` to the constructor.

---

### CONC-01 · 🟡 · 🟥 open · `BackgroundPollingExecutor` busy-loop without shutdown / except

Will matter once Q-01 realtime mode is alive.

---

### CONC-02 · 🟡 · 🟥 open · Cross-thread `Portfolio` mutation without locks

Same as CONC-01 — pre-emptive fix.

---

### IO-01 · 🟡 · 🟥 open · `FileLogger` opens file on every call

Still open.

---

## 🟢 Low

### IO-02 · 🟢 · 🟥 open · `GraphLogger2D` rewrites PNG on every log

Still open.

---

### DX-01 · 🟢 · 🟥 open · README stale / minimal

Still open. Per Q-11 the diploma needs an architecture section — README is a natural draft for it.

---

### DX-02 · 🟢 · 🟦 answered (iter-2) · `camelCase` methods in Python

Per Q-05: deliberate choice (3-person team, 2 of whom are ML-first, 1 (the user) is C# background and owns architecture). Status: **🟦 answered, won't change**. Could be re-classed ⬛ wontfix after consensus.

---

### DX-03 · 🟢 · 🟥 open · No `.gitignore`

Still open.

---

### DX-04 · 🟢 · 🟥 open · Emoji + Russian text in logs

Still open. Cosmetic.

---

### DX-05 · 🟢 · 🟥 open · `old/` directory in repo

Per Q-11: legacy code may need to remain for the diploma submission. Recommend moving to a `legacy/` folder and noting in README "reference only, superseded by `src/`".

---

### SEC-02 · 🟢 · 🟥 open · Dynamic `hasattr`/`getattr` probing in `RandomForestAlgo`

Still open.

---

### IO-03 · 🟢 · 🟥 open · `main.py` wipes `logs/` and `stats/` on every run

Per Q-11 the user's research artefacts live in `stats/`. **Severity upgrade considered → kept low** because the user manages this manually for now, but added IO-05 (new) — stats preservation policy.

---

## 🟠 / 🔴 — Cross-cutting (high+)

### IO-04 · 🟠 · 🟥 open · MOEX HTTP: no caching, retries, rate-limit handling

Still open. Caching alone would speed grid-search 10× and reduce MOEX load.

---

### TEST-01 · 🟠 · 🟥 open · Zero automated tests

Still open. Per Q-11 the team is 3 people and the diploma includes architecture — unit tests for VOs (`Asset`, `AssetPair`, `Range`, `ExecutionContext`, `Candle`) are 1-day work and substantially strengthen the architecture defense.

---

### TEST-02 · 🟡 · 🟥 open · No CI / linting / type-checking

Still open. Pyright already configured; just needs CI runner.

---

### SEC-01 · 🟠 · 🟦 answered (iter-2) · QUIK credentials handling unclear

Per TRADE-01 + Q-02: no broker is wired yet, so credentials are not an immediate risk. When QUIK is wired, the missing pieces are: `.env` template, `configLocal.py.example`, README section. Re-classified 🟦, will revisit when broker integration starts.

---

## 🔵 Open questions — now answered (iter-2)

All 11 questions answered by user on 2026-05-24. Verbatim record preserved in [`iterations/iteration-02.md`](iterations/iteration-02.md:1). Short summary:

| ID   | Status | Short answer                                                                                                                                                                                           | Triggered new findings            |
| ---- | ------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | --------------------------------- |
| Q-01 | 🟦     | Robot is end-to-end pipeline (DataSource → Predictor → Strategy → Assessor → Executor). Predictor real, Strategy/Assessor moc, brokers moc, but full flow must work. Realtime + backtest both planned. | ARCH-08                           |
| Q-02 | 🟦     | No fixed broker; QUIK is one candidate. Swap-in must be painless.                                                                                                                                      | TRADE-01 (linked)                 |
| Q-03 | 🟦     | Risk-management lives in Assessor — that is why Assessor exists.                                                                                                                                       | DOM-09                            |
| Q-04 | 🟦     | Currency only, for now.                                                                                                                                                                                | –                                 |
| Q-05 | 🟦     | `camelCase` is deliberate (3-person team, C# background).                                                                                                                                              | DX-02 (answered)                  |
| Q-06 | 🟦     | Portfolio is context-aware; capitalization depends on market state via `IExecutionContext`.                                                                                                            | RUN-04 (resolved)                 |
| Q-07 | 🟦     | Persistence desired but low priority.                                                                                                                                                                  | ML-07                             |
| Q-08 | 🟦     | Backtest abstraction is welcome; backtest is planned.                                                                                                                                                  | ARCH-08                           |
| Q-09 | 🟦     | Composite predictors = empirical ensemble symbiosis.                                                                                                                                                   | –                                 |
| Q-10 | 🟦     | Multi-asset planned, but next period.                                                                                                                                                                  | –                                 |
| Q-11 | 🟦     | Deadline 1 month. Diploma = research artefact + architecture chapter (UML, explanations). User owns architecture + predictors. May submit code excluding strategy/signals.                             | IO-05, DX-06, TEST-01 (priority↑) |

---

## ✨ New findings discovered in iteration 2

### ARCH-08 · 🟠 · 🟥 open (new iter-2) · No backtest/realtime separation; introduce `IClock` and `IBacktestScenario`

**Why now:** Q-01 confirms both backtest and realtime must work end-to-end on the same codebase. Q-08 confirms a backtest abstraction is welcome. Current `MockContextProvider` (see RUN-05) conflates real and elapsed time. Without an explicit clock + scenario, RUN-05/DOM-08/CONC-01 cannot be fixed cleanly.

**Recommended shape:**

- `IClock { now() -> int }` → `RealtimeClock` reads `time.time()`, `BacktestClock` advances on each candle.
- `IBacktestScenario { setup() ; tick() -> bool ; teardown() }` — owns the clock + a virtual `IMarket` that fills orders at backtest candle's open/close with configurable slippage/fees.
- Factory split: `BacktestFactory` vs. `LiveFactory`, both consuming the same `Strategy/Assessor/Predictor` impls.

**Supersedes:** ARCH-04 (the old "simulation vs production market" finding, now reframed concretely).

---

### DOM-09 · 🟠 · 🟥 open (new iter-2) · Assessor lacks explicit risk-cap configuration

**Why now:** Q-03 confirms Assessor owns risk. Current `HalfInAssessor` only does sizing (50% of cash) — no max-drawdown, no max-position, no per-trade loss cap, no kill-switch. The interface `IAssessor` does not enforce or even surface risk configuration.

**Recommended shape:**

- Extract a `IRiskPolicy { maxPositionFraction, maxDrawdown, killSwitch(portfolio, context) -> bool }`.
- `IAssessor.__init__(strategy, portfolio, riskPolicy, …)`.
- `HalfInAssessor` becomes a thin compositor of `FractionalSizing(0.5)` + a default `IRiskPolicy.unlimited()`.

This is needed even for moc-Assessor so the _architecture_ chapter of the diploma reflects the intent.

---

### ML-07 · 🟡 · 🟦 answered (new iter-2, deferred) · Model persistence — explicit deferral

**Why now:** Q-07 — persistence is wanted but low priority. Recording so it doesn't slip:

- `ITrainablePredictor.save(path)` / `.load(path)` (joblib for sklearn/xgboost; `keras.models.save_model` for Keras).
- `RandomForestAlgo._initNormalization` constants must be saved together with the model — otherwise loaded model is broken.

Status 🟦 deferred. Reopens automatically when ML-01 is scheduled.

---

### IO-05 · 🟡 · 🟥 open (new iter-2) · `stats/` directory should be preserved across runs (diploma artefact)

**Why now:** Q-11 — research results in `stats/` are diploma evidence. Current [`main.py`](../main.py:5) deletes them on each run (IO-03).

**Recommended fix:** on startup, **move** `logs/` and `stats/` to `logs.YYYY-MM-DD-hh-mm/` and `stats.YYYY-MM-DD-hh-mm/` before re-creating empty dirs. Keep last N (e.g. 20) and gzip older.

Subsumes IO-03 conceptually but kept separately (different fix scope).

---

## 🟩 Resolved archive

<a id="resolved-archive"></a>

> 6 items closed in iteration 2 (full text kept per never-delete rule). Also includes the historical state of RUN-05 / TRADE-01 / TRADE-03 before they were demoted — see [Demoted-from-Critical](#-demoted-from-critical-now--high--markers) for current status.

### RUN-01 · 🟩 resolved (iter-2) · `RandomForestPredictor/` lacked `__init__.py`

**Original:** [`src/Contracts/Predictor/__init__.py`](../src/Contracts/Predictor/__init__.py:1) imports `from .RandomForestPredictor.RandomForestPredictor import …` but the folder had no `__init__.py`, so `import Contracts` would fail (or rely on namespace-package coincidence).

**Resolution (iter-2):** verified — [`src/Contracts/Predictor/RandomForestPredictor/__init__.py`](../src/Contracts/Predictor/RandomForestPredictor/__init__.py:1) exists. ✅

---

### RUN-02 · 🔴 · 🟩 resolved (iter-2) · `_id` fields typed `str` but stored `uuid.UUID`

**Original:** [`Task`](../src/Entities/Task/Task.py:21), [`AAction`](../src/Entities/Action/AAction.py:15), [`APrediction`](../src/Entities/Prediction/APrediction.py:10), [`ASignal`](../src/Entities/Signal/ASignal.py:18), [`CandleSeries`](../src/Entities/CandleSeries/CandleSeries.py:14), [`TrimmedCandleSeries`](../src/Entities/CandleSeries/TrimmedCandleSeries.py:12), [`PredictionMeta`](../src/Entities/PredictionMeta/PredictionMeta.py:17) all assigned `uuid.uuid4()` to `_id: str`.

**Resolution (iter-2):** all six entities now do `self._id = str(uuid.uuid4())`. ✅

---

### RUN-03 · 🔴 · 🟩 resolved (iter-2) · `requirements.txt` incomplete (fixed by agent on user's request)

**Original:** missing `numpy`, `tensorflow`, `xgboost`, `scikit-learn`, `joblib`, `seaborn`, `scipy`, `pyzmq`; no pinning.

**Resolution (iter-2):** [`requirements.txt`](../requirements.txt:1) rewritten with reasonable version floors and upper bounds grouped by purpose (scientific / viz / ML / DL / networking / QUIK). Pin strategy: lower bound on minor version, upper bound on major to avoid silent breakage. ✅

---

### RUN-04 · 🔴 · 🟩 resolved (iter-2) · `RuntimePortfolio.getCapitalization` signature

**Original:** `IPortfolio.getCapitalization(self) -> float` but `RuntimePortfolio.getCapitalization(self, context)`. Liskov violation; `LoggedPortfolio` would `TypeError` on first call.

**Resolution (iter-2):** the **interface** [`IPortfolio.getCapitalization`](../src/Interfaces/Services/IPortfolio.py:21) now requires `context: IExecutionContext`, and [`LoggedPortfolio.getCapitalization`](../src/Services/Portfolio/LoggedPortfolio/LoggedPortfolio.py:21) forwards it. Decision aligned with Q-06 answer (portfolio capitalization is context-aware by design). ✅

---

### RUN-05 · 🔴 · 🟦 answered (iter-2) · `MockContextProvider` returns elapsed-time instead of Unix timestamp

> **User note (iter-2):** "чем оно может быть критично? Дополни карточку, оставим пока в списке."

**Why it is critical for the trading pipeline (extended justification):**

1. **Type lie:** [`IExecutionContext.getTimestamp() -> int`](../src/Interfaces/ValueObjects/IExecutionContext.py:1) is declared as `int` (intended: Unix seconds). [`MockContextProvider`](../src/Contracts/ContextProvider/MockContextProvider/MockContextProvider.py:11) stores `time.time()` (a `float`, Unix s) and emits `time.time() - self._startTimestamp` — a _float, elapsed-since-startup_. Two violations stacked: float-vs-int, and elapsed-vs-absolute.
2. **Trigger misfire:** [`TurnBackAction`](../src/Entities/Action/TurnBackAction.py:9) computes `self._triggerTimestamp = context.getTimestamp() + duration`. `duration = 15`. If `context.getTimestamp()` is `0.0023` (just-started), the action fires immediately on the second poll. If `MockContextProvider` lived for a while before the order, the action's trigger-time is in the _past_ and fires instantly.
3. **Inconsistency with real `IContextProvider`:** any future broker-backed implementation will return Unix seconds. Same code (Strategy/Assessor/Action) is consumed by _both_ — Mock vs. real — and silently behaves differently. The bug surfaces only when you switch from mock to live (worst possible time).
4. **Backtester collision (per Q-08):** backtest needs a _virtual_ clock that the data source advances. The current Mock conflates "now" with "elapsed-since-startup", which is neither real time nor virtual time — it's neither. A proper `IBacktestClock` (see new ARCH-08) is the right answer.

**Recommended fix:**

- Short term: make `MockContextProvider` return `int(time.time())`. Drop `_startTimestamp`.
- Medium term: introduce `IClock` abstraction; `MockContextProvider` and live providers both read a clock. Backtester injects a deterministic virtual clock.

Kept open at user's request; severity remains 🔴 because it silently corrupts every action's trigger window and breaks backtesting.

---

### TRADE-01 · 🔴 · 🟦 answered (iter-2) · No real broker integration

> **User note (iter-2):** "пока не планируем, робот в тестовой среде. Интерфейсы должны позволять без дублирования кода переключиться на реального брокера."

**Status:** confirmed scope decision. The current `PortfolioSyncMarket` is intentional for the diploma research scope.

**Architectural contract for "swap-in without duplication":** for the abstraction to hold, the following must remain interface-only and broker-agnostic:

- [`IMarket.execute(task)`](../src/Interfaces/Services/IMarket.py:1) — currently OK.
- [`IExecutionContext`](../src/Interfaces/ValueObjects/IExecutionContext.py:1) must grow to expose at least _bid/ask_, _balance_, _order status_ — see DOM-01.
- A new `IOrderState` (accepted / partial / filled / rejected / cancelled) — see TRADE-03.

Kept open as **🟦 answered**, will track follow-up under ARCH-08 (simulation-vs-live separation) and TRADE-03 (order state machine).

---

### TRADE-02 · 🔴 · 🟩 resolved (iter-2) · Fractional lot count silently accepted

**Original:** [`HalfInAssessor`](../src/Contracts/Assessor/HalfInAssessor/HalfInAssessor.py:25) computed `lotCount = volume / price / lotSize` as `float`, passed into `Task(lotCount: int)`, which silently stored a float.

**Resolution (iter-2):** now wrapped in [`int(...)`](../src/Contracts/Assessor/HalfInAssessor/HalfInAssessor.py:25). Real-world rejection (exchange refuses fractional lots) is now prevented at source. ✅

Follow-up captured separately as TRADE-06 (truncation policy — `int(...)` floors, but for `Down`-direction sell-half the floor can underreport; should be explicit).

---

### TRADE-03 · 🔴 · 🟦 answered (iter-2) · No order state machine between intended action and broker fills

> **User note (iter-2):** "согласен, оставим в списке."

Kept open as 🟦 answered. Will need a real implementation when TRADE-01 is unlocked.

**Cross-link:** the answer to Q-08 (backtest abstraction) implies the order state machine must be shared between simulated and live `IMarket`. Captured as part of ARCH-08.

---

---

## Change log (per iteration)

### Iteration 2 — 2026-05-24

**Resolved (🟩):** RUN-01, RUN-02, RUN-03 (agent-fixed), RUN-04, TRADE-02, ML-06 (duplicate of RUN-01).

**Answered (🟦):** RUN-05 (extended justification), TRADE-01, TRADE-03, ARCH-01, DOM-01, DOM-02, DOM-03, ML-01, TRADE-05, DX-02, SEC-01, Q-01…Q-11 (all 11).

**Superseded (🟪):** ARCH-04 → by ARCH-08.

**New (4):** ARCH-08, DOM-09, ML-07, IO-05.

**Open / unchanged:** ARCH-02, ARCH-03, ARCH-05, ARCH-06, ARCH-07, DOM-04, DOM-05, DOM-06, DOM-07, DOM-08, ML-02, ML-03, ML-04, ML-05, TRADE-04, CONC-01, CONC-02, IO-01, IO-02, IO-03, IO-04, DX-01, DX-03, DX-04, DX-05, TEST-01, TEST-02, SEC-02.

**Net total:** 51 → 55 (4 new, 0 deleted; 6 resolved but stay listed with 🟩 status per workflow rules).

### Iteration 1 — 2026-05-24

Initial audit. All 51 items opened. No statuses changed.
