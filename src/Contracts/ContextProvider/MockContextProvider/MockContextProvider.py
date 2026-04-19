import time
from Interfaces import IExecutionContext, IContextProvider, IAssetPair
from ValueObjects import ExecutionContext


class MockContextProvider(IContextProvider):
    _startTimestamp: int
    _assetPair: IAssetPair

    def __init__(self, assetPair: IAssetPair):
        self._startTimestamp = int(time.time())
        self._assetPair = assetPair

    def getContext(self, _: int) -> IExecutionContext:
        prices = dict[IAssetPair, float]()
        prices[self._assetPair] = 10

        return ExecutionContext(
            int(time.time()) - self._startTimestamp, prices, prices, prices, prices
        )
