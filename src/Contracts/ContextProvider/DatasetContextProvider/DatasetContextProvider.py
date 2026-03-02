import time
from Interfaces import IExecutionContext, IContextProvider, IAssetPair, IDataSource
from ValueObjects import ExecutionContext

class DatasetContextProvider(IContextProvider):
    _startTimestamp: int
    _assetPair: IAssetPair

    def __init__(self, assetPair: IAssetPair, dataset: IDataSource):
        self._startTimestamp = int(time.time())
        self._assetPair = assetPair
        self._dataset = dataset

    def getContext(self, timestamp: int) -> IExecutionContext:
        prices = dict[IAssetPair, float]()
        candle = self._dataset.getSeries().getByTimestamp(timestamp)
        if candle:
            prices[self._assetPair] = candle.getOpenPrice()
            return ExecutionContext(int(time.time()) - self._startTimestamp, prices)
        raise Exception("invalid candle exeption")
