from .IFactory import IFactory
from math import isfinite
from Contracts import (EWMAClosePredictor, RidgeClosePredictor, OpenCloseStrategy,
                       CorridorAssessor, DatasetContextProvider, HistoricalExecutor)
from Services import RuntimePortfolio, PortfolioSyncMarket


class CausalBacktestFactory(IFactory):
    """Two strategy implementations assembled from existing contract boundaries."""
    def __init__(self, dataSource, mode="algo", candlesCount=20, capital=100000,
                 commissionRate=0.0, slippageRate=0.0, predictorOptions=None, assessorOptions=None):
        if mode not in ("algo", "ml") or not isfinite(capital) or capital <= 0:
            raise ValueError("Expected mode algo/ml and positive capital")
        self._dataSource = dataSource
        pair = dataSource.getSeries().getAssetPair()
        predictor_type = EWMAClosePredictor if mode == "algo" else RidgeClosePredictor
        self._predictor = predictor_type(candlesCount, **(predictorOptions or {}))
        self._portfolio = RuntimePortfolio(pair.getBaseAsset())
        self._portfolio.deposit(capital)
        self._strategy = OpenCloseStrategy()
        self._assessor = CorridorAssessor(self._portfolio, **(assessorOptions or {}))
        self._contextProvider = DatasetContextProvider(pair, dataSource)
        self._market = PortfolioSyncMarket(self._portfolio, self._contextProvider, commissionRate, slippageRate)
        self._executor = HistoricalExecutor(self._market)

    def getDataSource(self):
        return self._dataSource

    def getPredictor(self):
        return self._predictor

    def getPortfolio(self):
        return self._portfolio

    def getStrategy(self):
        return self._strategy

    def getAssessor(self):
        return self._assessor

    def getContextProvider(self):
        return self._contextProvider

    def getMarket(self):
        return self._market

    def getExecutor(self):
        return self._executor
