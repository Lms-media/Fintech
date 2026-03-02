from Interfaces import IAssessor, IPortfolio, IContextProvider, DirectionType
from Entities import CandleSignal, ITurnBackAction, TurnBackAction

class CandleTestAssesor(IAssessor[CandleSignal, ITurnBackAction]):
    _portfolio: IPortfolio
    _contextProvider: IContextProvider

    def __init__(self, portfolio: IPortfolio, contextProvider: IContextProvider):
        self._portfolio = portfolio
        self._contextProvider = contextProvider

    def getAction(self, input: CandleSignal) -> ITurnBackAction:
        predictionCandle = input.getPrediction()
        previousCandle = input.previousCandles.getByIndex(input.previousCandles.getCount() - 1)
        if not previousCandle:
            raise ValueError("Invalid candle")
        
        context = self._contextProvider.getContext(previousCandle.getOpenTimestamp())
        assetPair = input.previousCandles.getAssetPair()

        if not self._portfolio.getBaseAsset() == assetPair.getBaseAsset():
            raise ValueError(f"Assessor's signal must match with portfolio with base asset, but portfolio base asset is {self._portfolio.getBaseAsset()} and signal base asset is {assetPair.getBaseAsset()}")

        price = context.getPrice(assetPair)

        if not price:
            raise ValueError(f"Assessor's context is missing folowing asset pair price: {assetPair}")

        print(price)
        print(self._portfolio.getCapitalization(context))
        if previousCandle.getOpenPrice() < predictionCandle.getClosePrice():
            lotToBuy = int((self._portfolio.getBaseAmount() * 0.4) / price)
            lotToBuy = lotToBuy if lotToBuy > 0 else 0
            print("buying:", lotToBuy)
            return TurnBackAction(input, assetPair, lotToBuy, True, context, 1, previousCandle.getOpenTimestamp(), predictionCandle.getOpenTimestamp() - previousCandle.getOpenTimestamp())
        if previousCandle.getOpenPrice() > predictionCandle.getClosePrice():
            lotToBuy = int((self._portfolio.getBaseAmount() * 0.4) / price)
            lotToBuy = lotToBuy if lotToBuy > 0 else 0
            print("selling:", lotToBuy)
            return TurnBackAction(input, assetPair, lotToBuy, False, context, 1, previousCandle.getOpenTimestamp(), predictionCandle.getOpenTimestamp() - previousCandle.getOpenTimestamp())

        return TurnBackAction(input, assetPair, 0, True, context, 1, previousCandle.getOpenTimestamp(), predictionCandle.getOpenTimestamp() - previousCandle.getOpenTimestamp())
