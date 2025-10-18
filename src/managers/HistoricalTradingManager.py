from src.managers.TradingManager import TradingManager
from src.data.DataSource import DataSource
from src.data.VirtualPortfolio import VirtualPortfolio
from datetime import datetime as dt
from src.strategies.Strategy import Strategy
from src.data.Candle import Candle


class HistoricalTradingManager(TradingManager):
    def __init__(
        self,
        instruments: dict[str, Strategy],
        chunkSize: int,
        dataSources: dict[str, DataSource],
        initialAmount: float,
        startDate: dt,
    ):
        self.dataSources = dataSources
        lotSizes: dict[str, float] = {}
        exchangeRates: dict[str, float] = {}
        self._currentCandles: dict[str, Candle] = {}
        for instrument in instruments:
            lotSizes[instrument] = 0.0
            lastCandle = dataSources[instrument].getSlice(
                dt.fromtimestamp(0), startDate
            )[-1]
            exchangeRates[instrument] = lastCandle.close
            self._currentCandles[instrument] = lastCandle
        self.virtualPortfolio = VirtualPortfolio(
            initialAmount, lotSizes, exchangeRates, startDate
        )
        self.instruments = instruments
        self.chunkSize = chunkSize

    def start(self):
        continueCount = 0
        while continueCount != len(self.instruments):
            continueCount = 0
            for instrument in self.instruments:
                if (
                    self.dataSources[instrument].nextCandle(
                        self._currentCandles[instrument]
                    )
                    is not None
                ):
                    quantity = self.instruments[instrument].predict(
                        self.dataSources[instrument].getSlice(
                            dt.fromtimestamp(0),
                            self._currentCandles[instrument].datetime,
                        )[: -self.chunkSize - 1 : -1][::-1]
                    )
                    self.processTestStep(quantity, instrument)
                else:
                    continueCount += 1
                    continue

    def processTestStep(self, quantity: int, instrument: str):
        currentCandle = self._currentCandles[instrument]

        if quantity == 0:
            self.virtualPortfolio.skip(instrument, currentCandle)
            self._currentCandles[instrument] = self.dataSources[instrument].nextCandle(
                currentCandle
            )
            return

        if quantity > 0:
            self.virtualPortfolio.buy(instrument, abs(quantity), currentCandle)
        else:
            self.virtualPortfolio.sell(instrument, abs(quantity), currentCandle)

        self._currentCandles[instrument] = self.dataSources[instrument].nextCandle(
            currentCandle
        )
