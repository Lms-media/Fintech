from Interfaces import IAssetPair, IDataSource, IPredictor, IStrategy, IAssessor, IExecutor, IMarket, IPortfolio, IContextProvider
from Factories import IFactory
from Entities import INextCandlePrediction, IDirectionSignal, ITurnBackAction
from Contracts import MockDataSource, LoggedDataSource, DummyPredictor, LoggedPredictor, DummyStrategy, LoggedStrategy, HalfInAssessor, LoggedAssessor, BackgroundPollingExecutor, MockContextProvider, LoggedContextProvider
from Services import PortfolioSyncMarket, LoggedMarket, RuntimePortfolio, FileLogger, TimestampedLogger

class DummyFactory(IFactory[INextCandlePrediction, IDirectionSignal, ITurnBackAction]):
    _assetPair: IAssetPair
    _dataSource: IDataSource
    _predictor: IPredictor
    _strategy: IStrategy
    _assessor: IAssessor
    _executor: IExecutor
    _market: IMarket
    _portfolio: IPortfolio
    _contextProvider: IContextProvider

    def __init__(self, assetPair: IAssetPair):
        dataSourceLogger = TimestampedLogger(FileLogger("logs/dataSource.log"))
        predictorLogger = TimestampedLogger(FileLogger("logs/predictor.log"))
        strategyLogger = TimestampedLogger(FileLogger("logs/strategy.log"))
        assessorLogger = TimestampedLogger(FileLogger("logs/assessor.log"))
        contextProviderLogger = TimestampedLogger(FileLogger("logs/contextProvider.log"))
        marketLogger = TimestampedLogger(FileLogger("logs/market.log"))

        dataSourceLogger.init()
        predictorLogger.init()
        strategyLogger.init()
        assessorLogger.init()
        contextProviderLogger.init()

        self._assetPair = assetPair
        self._contextProvider = LoggedContextProvider(MockContextProvider(self._assetPair), contextProviderLogger)
        self._dataSource = LoggedDataSource(MockDataSource(self._assetPair), dataSourceLogger)
        self._dataSource.init()
        self._predictor = LoggedPredictor(DummyPredictor(), predictorLogger)
        self._strategy = LoggedStrategy(DummyStrategy(), strategyLogger)
        self._portfolio = RuntimePortfolio(self._assetPair.getBaseAsset())
        self._portfolio.deposit(1000)
        self._market = LoggedMarket(PortfolioSyncMarket(self._portfolio, self._contextProvider), marketLogger)
        self._assessor = LoggedAssessor(HalfInAssessor(self._portfolio, self._contextProvider), assessorLogger)
        self._executor = BackgroundPollingExecutor(self._market, self._contextProvider)

    def getDataSource(self) -> IDataSource:
        return self._dataSource

    def getPredictor(self) -> IPredictor[INextCandlePrediction]:
        return self._predictor

    def getStrategy(self) -> IStrategy[INextCandlePrediction, IDirectionSignal]:
        return self._strategy

    def getAssessor(self) -> IAssessor[IDirectionSignal, ITurnBackAction]:
        return self._assessor

    def getExecutor(self) -> IExecutor:
        return self._executor

    def getMarket(self) -> IMarket:
        return self._market

    def getPortfolio(self) -> IPortfolio:
        return self._portfolio

    def getContextProvider(self) -> IContextProvider:
        return self._contextProvider
