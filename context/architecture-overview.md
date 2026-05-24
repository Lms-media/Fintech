# Architecture Overview (reconstructed)

> What I actually see in the repo on iteration 1, before any user clarification.
> This is a **descriptive** map, not a prescriptive one. Problems are tracked separately in [`findings.md`](findings.md).

## 1. Stated purpose

From [`README.md`](../README.md:1): _«Проект торгового бота, основанный на библиотеке QuikPy и API московской биржи iss.moex.com»_. The user adds: an autonomous trading robot that performs market actions for profit.

## 2. Top-level layout

```
/
├── main.py              # current entry point (ML retraining experiment, NOT live trading)
├── src/                 # the actual project (Clean-Architecture-ish layered codebase)
├── old/                 # legacy code — not imported by src/ (Robot.py, MLModule.py, ...)
├── Examples/            # standalone QuikPy demos (separate sys.path; uses ../QUIK)
├── QUIK/                # vendored QuikPy library + Lua scripts for QUIK terminal
├── requirements.txt     # 5 deps; missing many that the code actually imports
├── pyrightconfig.json   # Python 3.9, basic type-checking
└── README.md            # mentions a non-existent config.py / configLocal.py flow
```

## 3. Layered design inside `src/`

The architecture clearly aims at **Clean Architecture / Ports-and-Adapters**:

```
                    ┌─────────────────────────────────────────┐
                    │              UseCases                   │  ← orchestration
                    │  Infinite/SingleAction, PredictorTrain/  │
                    │  Test/Retrain/Visualize                  │
                    └───────────────┬─────────────────────────┘
                                    │ uses
              ┌─────────────────────┴────────────────────────┐
              │                Factories                     │  ← DI composition root
              │           MainFactory, DummyFactory          │
              └─────┬──────────────────────────────────┬─────┘
                    │                                  │
   ┌────────────────▼──────────────┐    ┌──────────────▼─────────────────┐
   │          Contracts            │    │            Services            │
   │ DataSource  Predictor         │    │  Market   Portfolio   Logger   │
   │ Strategy    Assessor          │    │                                │
   │ Executor    ContextProvider   │    │                                │
   └────────────────┬──────────────┘    └──────────────┬─────────────────┘
                    │                                  │
                    └────────────┬─────────────────────┘
                                 │
              ┌──────────────────▼──────────────────┐
              │      Entities & ValueObjects        │
              │  CandleSeries, Action, Task,         │
              │  Prediction, Signal, PredictionMeta │
              │  Asset, AssetPair, Candle, Range,    │
              │  ExecutionContext, TaskTrigger       │
              └──────────────────┬──────────────────┘
                                 │
              ┌──────────────────▼──────────────────┐
              │             Interfaces              │  ← abstract base classes for everything
              │  (mirrors the structure above)      │
              └─────────────────────────────────────┘
```

Every concrete class implements an interface in `src/Interfaces/...`; the package is split so that `Contracts/` (strategies, predictors, …) and `Services/` (market, portfolio, logger) are **adapter** layers around stable abstractions.

## 4. Core pipeline (intended)

For a single iteration the trading pipeline is composed by `IFactory` and driven by a UseCase:

```
DataSource.getSeries()
        │ ICandleSeries
        ▼
Predictor.predict(series)        ← APredictor = Algo + Adapter; trainable variants exist
        │ IPrediction (e.g. NextCandlePrediction)
        ▼
Strategy.getSignal(prediction)
        │ ISignal (e.g. DirectionSignal)
        ▼
Assessor.getAction(signal)       ← e.g. HalfInAssessor → TurnBackAction (buy now / sell after N sec)
        │ IAction (composed of ITask[])
        ▼
Executor.start(action)           ← BackgroundPollingExecutor: thread polls tasks,
                                   waits for ITaskTrigger, calls Market.execute(task)
        │
        ▼
Market.execute(task)             ← PortfolioSyncMarket: mutates Portfolio
                                   via ContextProvider price
```

Decorators (`LoggedDataSource`, `LoggedPredictor`, `LoggedStrategy`, `LoggedAssessor`, `LoggedExecutor`, `LoggedMarket`, `LoggedPortfolio`, `LoggedContextProvider`, `TimestampedLogger`) wrap every component for tracing.

## 5. Predictor sub-architecture

Strong sub-design — three roles per predictor:

| Role      | Interface                                                                           | Responsibility                                            |
| --------- | ----------------------------------------------------------------------------------- | --------------------------------------------------------- |
| Algo      | [`IPredictorAlgo`](../src/Interfaces/Contracts/Predictor/IPredictorAlgo.py:1)       | Pure computation: `IReadonlyCandleSeries → V`             |
| Adapter   | [`IPredictorAdapter`](../src/Interfaces/Contracts/Predictor/IPredictorAdapter.py:1) | Translate raw algo value `V` into domain `IPrediction`    |
| Predictor | [`IPredictor`](../src/Interfaces/Contracts/Predictor/IPredictor.py:1)               | Composition: `APredictor = Algo + Adapter + candlesCount` |

Implementations:

- `DummyPredictor` (placeholder)
- `IndicatorPredictor` family (SMA, WMA, EMA, MACD, RSI, Bollinger)
- `AbsolutePerceptronPredictor`, `RelativeMLPredictor` (LSTM, RNN, Perceptron, Memorizing variants)
- `PercentageMLPredictor`, `PercentageDeltaMLPredictor` (LSTM/RNN/Perceptron)
- `RandomForestPredictor` (actually wraps `XGBRegressor`, not RandomForest)
- Composite: `CompositeAvg`, `CompositeMedian`, `CompositeVoting`

`ITrainablePredictor` extends `IPredictor` with `addDatasetItem` / `train` / `clearDataset`, implemented by `ATrainablePredictor`.

## 6. Execution model

- `BackgroundPollingExecutor` spawns a `threading.Thread`, polls action tasks every 0.5 s.
- `ITaskTrigger` (`EmptyTaskTrigger`, `ScheduleTaskTrigger`, `CompositeTaskTrigger`) gates each task; `Action.update(context)` unlocks tasks whose triggers fire.
- `Market.execute` is currently `PortfolioSyncMarket` — **purely in-memory simulation**; there is no real broker integration despite `QUIK/` and `Examples/`.

## 7. Data sources

- `MoexCurrencyDataSource` — historic data via MOEX ISS public API (no auth).
- `MockDataSource` — 8 hard-coded candles.
- `LoggedDataSource` — decorator.
- No streaming / live datasource is wired in.

## 8. Configuration

- [`src/config/__init__.py`](../src/config/__init__.py:1) exposes a single hard-coded `assetPair = AssetPair(Asset('RUB',1), Asset('USD',10))`.
- README mentions `config.py` / `configLocal.py` — neither exists in `src/`. `.codeassistantignore` ignores `src/configLocal.py`, so it's apparently expected to be a local secret file, but no code reads it.

## 9. Logging

- `ILogger` is the seam.
- Implementations: `PrintLogger`, `FileLogger`, `TimestampedLogger` (decorator), `CompositeLogger`, `GraphLogger2D` (uses matplotlib, saves PNG every `log()` call), `ParamLogger` (turns scalar logs into x;y pairs for graphing).
- `main.py` brutally `shutil.rmtree('logs')` on every start.

## 10. What `main.py` actually does

It **does not** run the trading pipeline. It:

1. Wipes the `logs/` directory.
2. Configures two `MoexCurrencyDataSource` instances (train/test split by Unix timestamps).
3. Loops `i in range(77, 101)` and for each `i` creates a `PercentageDeltaMLPredictor(i)` and runs `PredictorRetrainUseCase` — i.e. it's an **ML grid-search experiment**, not a robot.

The Factory + Executor + Market + Portfolio composition exists but **is never instantiated from `main.py`**. A live-trading entry point is absent.

## 11. Things vendored / external

- `QUIK/QuikPy.py` + Lua scripts — a QUIK terminal bridge (Windows-only `core.dll`). Not used by `src/`.
- `Examples/` — standalone QuikPy demos with their own `sys.path` hacks.
- `old/` — previous monolithic implementation (`Robot.py`, `MLModule.py`, `TestRobot.py`).

## 12. Strong points (what's good)

- Clear DDD-flavoured layering with interfaces ↔ implementations split.
- Predictor Algo/Adapter/Predictor decomposition is elegant.
- Pervasive decorator-based logging is consistent.
- ValueObjects implement `__eq__` / `__hash__` / `__copy__` and validate invariants in constructors.
- `CandleSeries` enforces temporal continuity on append.
