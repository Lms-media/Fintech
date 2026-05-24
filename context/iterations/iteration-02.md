# Iteration 2 — 2026-05-24

> Workflow rule: nothing is deleted between iterations; only statuses change. New findings are appended with justification.

## Inputs from user

User addressed all 8 critical findings between iter-1 and iter-2:

- **RUN-01** "Исправил, проверь" → verified ✅
- **RUN-02** "Исправил, проверь" → verified ✅ (all 6 entities)
- **RUN-03** "Исправь сам, на свое усмотрение, главное чтобы работало и у всех" → agent fixed ✅
- **RUN-04** "Исправил, проверь" → verified ✅ (interface signature updated, see Q-06)
- **RUN-05** "Чем оно может быть критично? Дополни карточку, оставим в списке" → card extended with 4-point justification, kept 🟦 answered
- **TRADE-01** "Пока не планируем, в тестовой среде. Интерфейсы должны позволять без дублирования кода переключиться на реального брокера" → scope decision recorded; architectural contract for swap-in documented
- **TRADE-02** "Исправил, проверь" → verified ✅ (`int(...)`)
- **TRADE-03** "Согласен, оставим в списке" → kept 🟦 answered, linked to ARCH-08

## Questions answered (verbatim where useful)

### Q-01 — Pipeline scope

> "DataSource → Predictor → Strategy → Assessor → Executor. Predictor работает на реальных алгоритмах, есть User Flows для тестирования его работы. Этапы Strategy и Accessor — моковые, с простейшими алгоритмами, чтобы проверить, что ничего нигде не ломается. Реальные провайдеры рыночных услуг также не подключены, но их внедрение в будущем должно быть безболезненным. Во всех остальных местах должен быть готов костяк с минимальной, но рабочей реализацей. Весь целиковый флоу по торговле (хоть и ненастоящей) должен работать. Хотим в будущем торговать и в realtime режиме и в backtesting + другие различные тесты."

→ Triggers **ARCH-08** (clock + scenario abstraction).

### Q-02 — Broker

> "Не зафиксирован, не планируем фиксировать. Внедрение брокеров должно быть безболезненным."

### Q-03 — Risk

> "Risk менеджмент однозначно должен быть в Accessor. Более того, он создавался именно для этого."

→ Triggers **DOM-09** (explicit `IRiskPolicy`). Closes DOM-02 / DOM-03 as design decisions.

### Q-04 — Asset classes

> "Пока планируем только валюты, даже не задумывались о другом."

### Q-05 — `camelCase`

> "У нас в команде 3 человека и все привыкли работать на C#. Python выбрали 2 остальных человека (ML), я же занимаюсь архитектурой (ну и предсказателями)."

→ DX-02 closed as 🟦 (stylistic, deliberate).

### Q-06 — `IPortfolio` context

> "Контекст нужен. Портфель должен выдавать капитализацию для определенного состояния рынка."

→ Closes RUN-04 design question; interface fix is correct.

### Q-07 — Persistence

> "Не задумывались. Сейчас думаю, что хорошо было бы иметь хранилище (движок не выбираю). Приоритет низкий."

→ Triggers **ML-07** (persistence, deferred).

### Q-08 — Backtest

> "Хорошая идея, можно добавить такую абстракцию. Бэктестинг мы однозначно планируем проводить."

→ Triggers **ARCH-08**.

### Q-09 — Composite predictors

> "Мотивация в скрещивании различных предсказателей с целью получения их результативного симбиоза, результаты которого будут лучше каждого из скрещивающихся предсказателей в отдельности."

### Q-10 — Multi-asset

> "Планируем, но в следующем периоде. Пока что можем остановиться на одном инструменте."

### Q-11 — Diploma context

> "Сроки — месяц. Результат — исследование (уже провёл, в `stats/`) при помощи текущего проекта. Я работаю только с предсказателями. Другие участники — стратегии и ассессоры. Сейчас навожу архитектурный порядок, потому что в работу нужно будет приложить исходный код (возможно даже без strategy/signal). В диплом хочу добавить раздел с архитектурой (UML, объяснения)."

→ Triggers **IO-05** (stats preservation), reinforces **TEST-01** priority (architecture chapter benefits from VO tests).

## Agent actions taken

### Code changes

- [`requirements.txt`](../../requirements.txt:1) — full rewrite with version-bounded entries: numpy, pandas, scipy, matplotlib, seaborn, scikit-learn, xgboost, joblib, keras, tensorflow, requests, pytz, pyzmq.

### Documentation changes

- [`context/findings.md`](../findings.md:1) — status updates for 17 items, full Q-answers section, 4 new findings appended.
- [`context/report.html`](../report.html:1) — status badges updated; KPI summary refreshed; resolution notes inlined; new section "Iteration 2 — added".
- [`context/iterations/iteration-02.md`](iteration-02.md:1) — this file.

## Decisions & open architectural questions for iter-3

1. **Backtest architecture (ARCH-08).** Recommended: introduce `IClock` + `IBacktestScenario` + split factories. Worth a dedicated UML diagram for the diploma's architecture chapter.
2. **Risk policy (DOM-09).** Recommended: extract `IRiskPolicy` as separate port; pass into `IAssessor` constructor.
3. **Entry points (new DX-06 candidate).** Currently `main.py` is grid-search-only. Recommend at least three small entry points: `bin/train.py`, `bin/backtest.py`, `bin/live.py` — each wires the appropriate factory. Useful for the diploma's chapter to show what the application "does".
4. **`stats/` preservation policy (IO-05).** Recommended: rename-on-startup instead of `rmtree`.
5. **ML-03 — research integrity.** Highest-impact open item for the diploma. Decide: rolling-window CV vs. explicit "online learning under drift" framing. The result matters for how the research chapter is defensible.

## Suggested focus for iteration 3

1. **ARCH-08 skeleton** (1-2 days): `IClock`, `BacktestClock`, `RealtimeClock`, `BacktestScenario` wiring. Implicitly closes RUN-05 and DOM-08.
2. **DOM-09 skeleton** (½ day): `IRiskPolicy.unlimited()` + retrofit into `HalfInAssessor`.
3. **ML-03 decision** (no code, 1 hour discussion): rolling-window CV vs. online-drift framing — this gates research chapter writing.
4. **DX-06 entry points** (½ day): three thin scripts.
5. **TEST-01 starter** (½ day): pytest setup + tests for `Asset`, `AssetPair`, `Range`, `Candle`. Doesn't need to cover Predictor.

Cost ≈ 4 working days for an architectural-chapter-ready codebase.
