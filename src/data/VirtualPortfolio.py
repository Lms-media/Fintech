from src.data.PortfolioState import PortfolioSate
from datetime import datetime as dt
from src.data.Candle import Candle


class VirtualPortfolio:
    def __init__(
        self,
        initialBaseAmount: float,
        lotSizes: dict[str, float],
        exchangeRates: dict[str, float],
        initialDate: dt = dt.now(),
    ):
        self._lotSizes = lotSizes
        self._currentState = PortfolioSate(
            initialBaseAmount, lotSizes, initialDate, exchangeRates
        )
        self._history: list[PortfolioSate] = []

    def getCurrentState(self) -> PortfolioSate:
        return self._currentState

    def buy(self, lots: dict[str, float], candles: dict[str, Candle]):
        newAmount = self._currentState.amount
        newAssets = self._currentState.assets
        newRates = self._currentState.exchangeRates
        for lot in lots:
            if (
                lots[lot] <= candles[lot].volume
                and newAmount >= lots[lot] * candles[lot].close
            ):
                newAmount -= lots[lot] * candles[lot].close
                newAssets[lot] = newAssets.get(lot, 0) + lots[lot]
                newRates[lot] = candles[lot].close
        self._history.append(self._currentState)
        self._currentState = PortfolioSate(
            newAmount, newAssets, candles[lot].datetime, newRates
        )

    def sell(self, lots: dict[str, float], candles: dict[str, Candle]):
        newAmount = self._currentState.baseAmount
        newAssets = self._currentState.assets
        newRates = self._currentState.exchangeRates
        for lot in lots:
            if newAssets.get(lot, 0) >= lots[lot]:
                newAmount += lots[lot] * candles[lot].close
                newAssets[lot] = newAssets.get(lot, 0) - lots[lot]
                newRates[lot] = candles[lot].close
        self._history.append(self._currentState)
        self._currentState = PortfolioSate(
            newAmount, newAssets, candles[lot].datetime, newRates
        )

    def skip(self, candles: dict[str, Candle]):
        newRates = self._currentState.exchangeRates
        datetime = dt.now()
        for asset in candles:
            datetime = candles[asset].datetime
            newRates[asset] = candles[asset].close
        self._history.append(self._currentState)
        self._currentState = PortfolioSate(
            self._currentState.amount, self._currentState.assets, datetime, newRates
        )
