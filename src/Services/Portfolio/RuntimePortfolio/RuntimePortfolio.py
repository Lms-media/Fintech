from Interfaces import IPortfolio, IAsset, IExecutionContext
from ValueObjects import AssetPair

class RuntimePortfolio(IPortfolio):
    _baseAsset: IAsset
    _baseAmount: float
    _contents: dict[IAsset, float]

    def __init__(self, baseAsset: IAsset):
        self._baseAsset = baseAsset
        self._baseAmount = 0

    def getBaseAsset(self) -> IAsset:
        return self._baseAsset

    def getAssetAmount(self, asset) -> float:
        if asset in self._contents:
            return self._contents[asset]

        return 0

    def getBaseAmount(self) -> float:
        return self._baseAmount

    def getCapitalization(self, context: IExecutionContext) -> float:
        capitalization = 0
        for asset in self._contents:
            assetPair = AssetPair(self._baseAsset, asset)
            price = context.getPrice(assetPair)
            if not price:
                raise ValueError(f"Unable to get portfolio capitalization, context doesn't have price for {assetPair}")
            capitalization += price

        return capitalization

    def buyAsset(self, asset: IAsset, lotCount: int, price: float) -> None:
        if asset in self._contents:
            self._contents[asset] += lotCount * asset.getLotSize()
        else:
            self._contents[asset] = lotCount * asset.getLotSize()

        self._baseAmount -= lotCount * asset.getLotSize() * price

    def sellAsset(self, asset: IAsset, lotCount: int, price: float) -> None:
        if asset in self._contents:
            self._contents[asset] -= lotCount * asset.getLotSize()
        else:
            self._contents[asset] = -lotCount * asset.getLotSize()

        self._baseAmount += lotCount * asset.getLotSize() * price

    def deposit(self, amount: float) -> None:
        self._baseAmount += amount

    def withdraw(self, amount: float) -> None:
        self._baseAmount -= amount
