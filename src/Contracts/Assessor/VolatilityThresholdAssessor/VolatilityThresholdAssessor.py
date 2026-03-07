from Interfaces import IAssessor, IPortfolio, IContextProvider, DirectionType
from Entities import CandleSignal, ITurnBackAction, TurnBackAction

class VolatilityThresholdAssessor(IAssessor[CandleSignal, ITurnBackAction]):
    _portfolio: IPortfolio
    _contextProvider: IContextProvider
    _thresholdCoef: float

    def __init__(self, portfolio: IPortfolio, contextProvider: IContextProvider, thresholdCoef: float):
        self._portfolio = portfolio
        self._contextProvider = contextProvider
        self._thresholdCoef = thresholdCoef

    def getAction(self, input: CandleSignal) -> ITurnBackAction:
        predictionCandle = input.getPrediction()
        previousCandle = input.previousCandles.getByIndex(input.previousCandles.getCount() - 1)
        if not previousCandle:
            raise ValueError("Invalid candle")
        
        context = self._contextProvider.getContext(previousCandle.getOpenTimestamp())
        assetPair = input.previousCandles.getAssetPair()

        nextCandle = self._contextProvider.getNextCandle(previousCandle)
        if not nextCandle:
            raise ValueError("Invalid next candle") 

        if not self._portfolio.getBaseAsset() == assetPair.getBaseAsset():
            raise ValueError(f"Assessor's signal must match with portfolio with base asset, but portfolio base asset is {self._portfolio.getBaseAsset()} and signal base asset is {assetPair.getBaseAsset()}")

        price = context.getPrice(assetPair)
        predictedPrice = predictionCandle.getOpenPrice()

        if not price:
            raise ValueError(f"Assessor's context is missing folowing asset pair price: {assetPair}")

        print(self._portfolio.getCapitalization(context))
        print(price)
        print("predicted:", predictedPrice)
        
        sum_ranges = 0
        for i in range(input.previousCandles.getCount()):
            candle = input.previousCandles.getByIndex(i)
            if candle:
                sum_ranges += (candle.getHighPrice() - candle.getLowPrice())
        sum_ranges /= input.previousCandles.getCount()
        
        predictedDiff = predictedPrice - price
        threshold = sum_ranges * self._thresholdCoef
        
        if abs(predictedDiff) > threshold:
            lotToBuy = int(((self._portfolio.getBaseAmount() * 0.4) / sum_ranges) / price)
            lotToBuy = lotToBuy if lotToBuy > 0 else 0
            if predictedDiff > 0:
                print("buying:", lotToBuy)
                return TurnBackAction(input, assetPair, lotToBuy, True, context, 1, previousCandle.getOpenTimestamp(), nextCandle.getOpenTimestamp() - previousCandle.getOpenTimestamp())
            else:
                print("selling:", lotToBuy)
                return TurnBackAction(input, assetPair, lotToBuy, False, context, 1, previousCandle.getOpenTimestamp(), nextCandle.getOpenTimestamp() - previousCandle.getOpenTimestamp())

        print("skipping")
        return TurnBackAction(input, assetPair, 0, True, context, 1, previousCandle.getOpenTimestamp(), nextCandle.getOpenTimestamp() - previousCandle.getOpenTimestamp())
