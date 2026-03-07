from Interfaces import IAssessor, IPortfolio, IContextProvider, DirectionType
from Entities import CandleSignal, ITurnBackAction, TurnBackAction

class RSIFilterAssessor(IAssessor[CandleSignal, ITurnBackAction]):
    _portfolio: IPortfolio
    _contextProvider: IContextProvider
    _thresholdCoef: float
    _upper_rsi: int
    _lower_rsi: int

    def __init__(self, portfolio: IPortfolio, contextProvider: IContextProvider, upper_rsi: int, lower_rsi: int):
        self._portfolio = portfolio
        self._contextProvider = contextProvider
        self._upper_rsi = upper_rsi
        self._lower_rsi = lower_rsi

    def _calculate_rsi(self, prices: list[float]) -> float:
        gains = []
        losses = []
        
        for i in range(1, len(prices)):
            diff = prices[i] - prices[i-1]
            if diff > 0:
                gains.append(diff)
                losses.append(0)
            else:
                gains.append(0)
                losses.append(abs(diff))
        
        avg_gain = sum(gains) /len(prices)
        avg_loss = sum(losses) /len(prices)
        
        if avg_loss == 0:
            return 100
        
        rs = avg_gain / avg_loss
        return 100 - (100 / (1 + rs))

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
        
        prices = []
        for i in range(input.previousCandles.getCount()):
            candle = input.previousCandles.getByIndex(i)
            if candle:
                prices.append(candle.getOpenPrice())
        rsi_value = self._calculate_rsi(prices)
        lotToBuy = int((self._portfolio.getBaseAmount() * 0.4) / price)
        print("rsi:", rsi_value)
        
        if predictedPrice > price and rsi_value < self._upper_rsi:
            if rsi_value < self._lower_rsi:
                lotToBuy *= 1.5
            lotToBuy = lotToBuy if lotToBuy > 0 else 0
            print("buying:", lotToBuy)
            return TurnBackAction(input, assetPair, int(lotToBuy), True, context, 1, previousCandle.getOpenTimestamp(), nextCandle.getOpenTimestamp() - previousCandle.getOpenTimestamp())
        if predictedPrice < price and rsi_value > self._lower_rsi:
            if rsi_value > self._upper_rsi:
                lotToBuy *= 1.5
            lotToBuy = lotToBuy if lotToBuy > 0 else 0
            print("selling:", lotToBuy)
            return TurnBackAction(input, assetPair, int(lotToBuy), False, context, 1, previousCandle.getOpenTimestamp(), nextCandle.getOpenTimestamp() - previousCandle.getOpenTimestamp())

        print("skipping")
        return TurnBackAction(input, assetPair, 0, True, context, 1, previousCandle.getOpenTimestamp(), nextCandle.getOpenTimestamp() - previousCandle.getOpenTimestamp())
