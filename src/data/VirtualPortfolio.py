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

    def buy(self, lot: str, lotSize: float, candle: Candle):
        newAmount = self._currentState.amount
        newAssets = self._currentState.assets.copy()
        newRates = self._currentState.exchangeRates.copy()
        if (
            lotSize <= candle.volume
            and newAmount >= lotSize * candle.close
        ):
            newAmount -= lotSize * candle.close
            newAssets[lot] = newAssets.get(lot, 0) + lotSize
            newRates[lot] = candle.close
        self._history.append(self._currentState)
        self._currentState = PortfolioSate(
            newAmount, newAssets, candle.datetime, newRates
        )

    def sell(self, lot: str, lotSize: float, candle: Candle):
        newAmount = self._currentState.amount
        newAssets = self._currentState.assets.copy()
        newRates = self._currentState.exchangeRates.copy()
        if newAssets.get(lot, 0) >= lotSize:
            newAmount += lotSize * candle.close
            newAssets[lot] = newAssets.get(lot, 0) - lotSize
            newRates[lot] = candle.close
        self._history.append(self._currentState)
        self._currentState = PortfolioSate(
            newAmount, newAssets, candle.datetime, newRates
        )

    def skip(self, lot: str, candle: Candle):
        newRates = self._currentState.exchangeRates.copy()
        datetime = candle.datetime
        newRates[lot] = candle.close
        self._history.append(self._currentState)
        self._currentState = PortfolioSate(
            self._currentState.amount, self._currentState.assets.copy(), datetime, newRates
        )
