from Interfaces import IAssessor, IPortfolio, IContextProvider, DirectionType
from Entities import CandleSignal, ITurnBackAction, TurnBackAction

class CandleTestAssesor(IAssessor[CandleSignal, ITurnBackAction]):
    _portfolio: IPortfolio
    _contextProvider: IContextProvider

    def __init__(self, portfolio: IPortfolio, contextProvider: IContextProvider):
        self._portfolio = portfolio
        self._contextProvider = contextProvider

    def getAction(self, input: CandleSignal) -> ITurnBackAction:
        volume = self._portfolio.getBaseAmount() / 2
        assetPair = input.previousCandles.getAssetPair()

        if not self._portfolio.getBaseAsset() == assetPair.getBaseAsset():
            raise ValueError(f"Assessor's signal must match with portfolio with base asset, but portfolio base asset is {self._portfolio.getBaseAsset()} and signal base asset is {assetPair.getBaseAsset()}")

        price = self._contextProvider.getContext().getPrice(assetPair)

        if not price:
            raise ValueError(f"Assessor's context is missing folowing asset pair price: {assetPair}")

        lotCount = volume / price / assetPair.getQuoteAsset().getLotSize()
        lotToBuy = int((self._portfolio.getCapitalization(self._contextProvider.getContext()) * 0.4) / price)
        
        if input.getPrediction().getOpenPrice() < input.getPrediction().getClosePrice():
            self._portfolio.buyAsset(assetPair.getQuoteAsset(), lotToBuy, price)
        if input.getPrediction().getOpenPrice() > input.getPrediction().getClosePrice():
            self._portfolio.sellAsset(assetPair.getQuoteAsset(), lotToBuy, price)

        return TurnBackAction(input, assetPair, int(lotCount), True, self._contextProvider.getContext(), 15)
