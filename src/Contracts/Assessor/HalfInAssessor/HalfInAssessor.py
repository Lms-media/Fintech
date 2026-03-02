from Interfaces import IAssessor, IPortfolio, IContextProvider, DirectionType
from Entities import IDirectionSignal, ITurnBackAction, TurnBackAction

class HalfInAssessor(IAssessor[IDirectionSignal, ITurnBackAction]):
    _portfolio: IPortfolio
    _contextProvider: IContextProvider

    def __init__(self, portfolio: IPortfolio, contextProvider: IContextProvider):
        self._portfolio = portfolio
        self._contextProvider = contextProvider

    def getAction(self, input: IDirectionSignal) -> ITurnBackAction:
        volume = self._portfolio.getBaseAmount() / 2
        meta = input.getPrediction().getMeta()
        assetPair = meta.getCandleSeries().getAssetPair()

        if not self._portfolio.getBaseAsset() == assetPair.getBaseAsset():
            raise ValueError(f"Assessor's signal must match with portfolio with base asset, but portfolio base asset is {self._portfolio.getBaseAsset()} and signal base asset is {assetPair.getBaseAsset()}")

        price = self._contextProvider.getContext(input.getTimestamp()).getPrice(assetPair)

        if not price:
            raise ValueError(f"Assessor's context is missing folowing asset pair price: {assetPair}")

        lotCount = volume / price / assetPair.getQuoteAsset().getLotSize()

        return TurnBackAction(
            input, 
            assetPair,
            int(lotCount),
            input.getDirection() == DirectionType.Up,
            self._contextProvider.getContext(input.getTimestamp()),
            15, 
            meta.getCandleSeries().getByTimestamp(input.getTimestamp()).getOpenTimestamp(),
            meta.getCandleSeries().getByTimestamp(input.getTimestamp()).getInterval().value
        )
