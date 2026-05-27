# Glossary / Accumulated Project Knowledge

> Filled in as the user answers questions across iterations. Empty placeholders are intentional.

## Domain

| Term       | Meaning (confirmed)                                            |
| ---------- | -------------------------------------------------------------- |
| Asset      | _to confirm in Q-06_                                           |
| AssetPair  | base/quote pair, e.g. RUB/USD                                  |
| Lot        | _to confirm in Q-06_                                           |
| Candle     | OHLC + volume + open-timestamp + interval                      |
| Prediction | model's forecast of the next candle                            |
| Signal     | strategy's decision (e.g. direction) derived from a prediction |
| Action     | a composite trading intent (1+ Tasks with triggers)            |
| Task       | atomic order: buy/sell N lots of asset pair when trigger fires |
| Trigger    | predicate over `ExecutionContext` that unlocks a task          |

## External systems

| System              | Role                                          | Confirmed?                             |
| ------------------- | --------------------------------------------- | -------------------------------------- |
| QUIK                | broker terminal (Lua + ZMQ bridge in `QUIK/`) | _Q-01_                                 |
| MOEX ISS            | public market-data HTTP API (`iss.moex.com`)  | yes (used in `MoexCurrencyDataSource`) |
| Tinkoff / Finam / … | candidate broker APIs                         | _Q-03_                                 |

## Project decisions (to be filled by user)

| Decision                             | Status         | Source |
| ------------------------------------ | -------------- | ------ |
| Naming style (PascalCase, camelCase) | _intentional?_ | DX-02  |
| Backtest vs. live separation         | _undecided_    | Q-05   |
| Persistence layer                    | _none yet_     | Q-10   |
| Risk model                           | _none yet_     | Q-08   |
