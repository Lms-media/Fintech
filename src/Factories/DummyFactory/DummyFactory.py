from Interfaces import IAssetPair, IDataSource, IPredictor, IStrategy, IAssessor, IExecutor, IMarket, IPortfolio, IContextProvider
from Factories import IFactory
from Entities import INextCandlePrediction, IDirectionSignal, ITurnBackAction
from Contracts import MockDataSource, DummyPredictor, DummyStrategy, HalfInAssessor, BackgroundPollingExecutor, MockContextProvider
from Services import LogMarket, RuntimePortfolio

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
        self._assetPair = assetPair
        self._dataSource = MockDataSource(self._assetPair)
        self._predictor = DummyPredictor()
        self._strategy = DummyStrategy()
        self._market = LogMarket()
        self._portfolio = RuntimePortfolio(self._assetPair.getBaseAsset())
        self._contextProvider = MockContextProvider(self._assetPair)
        self._assessor = HalfInAssessor(self._portfolio, self._contextProvider.getContext())
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
