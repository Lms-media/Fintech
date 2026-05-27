from Interfaces import IContextProvider
from ValueObjects import Asset, AssetPair, ExecutionContext

class MockedContextProvider(IContextProvider):
    def __init__(self, assetPair: AssetPair, price: float):
        assetPair = AssetPair(Asset("USD", 10), Asset("RUB", 1))
        self.context = ExecutionContext(0, { assetPair: 100 })

    def getContext(self):
        return self.context
