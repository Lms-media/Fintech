from Interfaces import IAssessor, IPortfolio, IContextProvider, DirectionType
from Entities import CandleSignal, ITurnBackAction, LimitBackAction, TurnBackAction
import capitalizationData


class VolatilityCorridorAssessor(IAssessor[CandleSignal, ITurnBackAction]):
    _portfolio: IPortfolio
    _contextProvider: IContextProvider
    _thresholdCoef: float
    _profitCoef: float
    _stopCoef: float

    def __init__(
        self,
        portfolio: IPortfolio,
        contextProvider: IContextProvider,
        thresholdCoef: float,
        profitCoef: float,
        stopCoef: float
    ):
        self._portfolio = portfolio
        self._contextProvider = contextProvider
        self._thresholdCoef = thresholdCoef
        self._profitCoef = profitCoef
        self._stopCoef = stopCoef

    def getAction(self, input: CandleSignal) -> ITurnBackAction:
        predictionCandle = input.getPrediction()
        previousCandle = input.previousCandles.getByIndex(
            input.previousCandles.getCount() - 1
        )
        if not previousCandle:
            raise ValueError("Invalid candle")

        context = self._contextProvider.getContext(previousCandle.getOpenTimestamp())
        assetPair = input.previousCandles.getAssetPair()

        nextCandle = self._contextProvider.getNextCandle(previousCandle)
        if not nextCandle:
            raise ValueError("Invalid next candle")

        if not self._portfolio.getBaseAsset() == assetPair.getBaseAsset():
            raise ValueError(
                f"Assessor's signal must match with portfolio with base asset, but portfolio base asset is {self._portfolio.getBaseAsset()} and signal base asset is {assetPair.getBaseAsset()}"
            )

        price = context.getPrice(assetPair)
        predictedPrice = predictionCandle.getOpenPrice()

        if not price:
            raise ValueError(
                f"Assessor's context is missing folowing asset pair price: {assetPair}"
            )

        print(self._portfolio.getCapitalization(context))
        capitalizationData.cap.append(self._portfolio.getCapitalization(context))
        capitalizationData.actual.append(nextCandle.getOpenPrice())
        capitalizationData.predictions.append(predictionCandle.getOpenPrice())
        print(price)
        print("predicted:", predictedPrice)

        sum_ranges = self.getATR(input)

        predictedDiff = predictedPrice - price
        threshold = sum_ranges * self._thresholdCoef

        if abs(predictedDiff) > threshold:
            lotToBuy = int(
                ((self._portfolio.getBaseAmount() * 0.4) / sum_ranges) / price
            )
            lotToBuy = lotToBuy if lotToBuy > 0 else 0
            if predictedDiff > 0:
                highLimit = max(predictedPrice, price + sum_ranges * self._profitCoef) # 2.0
                lowLimit = price - (sum_ranges * self._stopCoef) # 1.2
                print("buying:", lotToBuy)
                return LimitBackAction(
                    input,
                    assetPair,
                    lotToBuy,
                    True,
                    context,
                    1,
                    previousCandle.getOpenTimestamp(),
                    nextCandle.getOpenTimestamp() - previousCandle.getOpenTimestamp(),
                    highLimit,
                    lowLimit
                )
            else:
                highLimit = price + (sum_ranges * self._stopCoef)
                lowLimit = min(predictedPrice, price - sum_ranges * self._profitCoef)
                print("selling:", lotToBuy)
                return LimitBackAction(
                    input,
                    assetPair,
                    lotToBuy,
                    False,
                    context,
                    1,
                    previousCandle.getOpenTimestamp(),
                    nextCandle.getOpenTimestamp() - previousCandle.getOpenTimestamp(),
                    highLimit,
                    lowLimit
                )

        print("skipping")
        return TurnBackAction(
            input,
            assetPair,
            0,
            True,
            context,
            1,
            previousCandle.getOpenTimestamp(),
            nextCandle.getOpenTimestamp() - previousCandle.getOpenTimestamp(),
        )

    def getATR(self, input, period: int = 14):
        sum_ranges = 0
        count = input.previousCandles.getCount()
        for i in range(count - period, count):
            candle = input.previousCandles.getByIndex(i)
            prevCandle = input.previousCandles.getByIndex(i - 1)
            if candle:
                hight_low_delta = candle.getHighPrice() - candle.getLowPrice()
                hight_close_delta = (
                    abs(candle.getHighPrice() - prevCandle.getClosePrice())
                    if prevCandle
                    else hight_low_delta - 1
                )
                low_close_delta = (
                    abs(candle.getLowPrice() - prevCandle.getClosePrice())
                    if prevCandle
                    else hight_low_delta - 1
                )
                sum_ranges += max(hight_low_delta, hight_close_delta, low_close_delta)
        sum_ranges /= period
        return sum_ranges
