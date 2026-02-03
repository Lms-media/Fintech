from Interfaces import IAssessor, IPortfolio, IExecutionContext, DirectionType
from Entities import IDirectionSignal, ITurnBackAction, TurnBackAction

class HalfInAssessor(IAssessor[IDirectionSignal, ITurnBackAction]):
    _portfolio: IPortfolio
    _context: IExecutionContext

    def __init__(self, portfolio: IPortfolio, context: IExecutionContext):
        self._portfolio = portfolio
        self._context = context

    def getAction(self, input: IDirectionSignal) -> ITurnBackAction:
        volume = self._portfolio.getBaseAmount() / 2
        assetPair = input.getPrediction().getCandleSeries().getAssetPair()

        if not self._portfolio.getBaseAsset() == assetPair.getBaseAsset():
            raise ValueError(f"Assessor's signal must match with portfolio with base asset, but portfolio base asset is {self._portfolio.getBaseAsset()} and signal base asset is {assetPair.getBaseAsset()}")

        price = self._context.getPrice(assetPair)

        if not price:
            raise ValueError(f"Assessor's context is missing folowing asset pair price: {assetPair}")

        lotCount = volume / price / assetPair.getQuoteAsset().getLotSize()

        return TurnBackAction(input, assetPair, lotCount, input.getDirection() == DirectionType.Up, self._context.getTimestamp() + 15)
