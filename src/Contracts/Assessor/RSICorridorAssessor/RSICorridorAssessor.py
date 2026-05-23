from Interfaces import IAssessor, IPortfolio, IContextProvider, DirectionType
from Entities import CandleSignal, ITurnBackAction, TurnBackAction, LimitBackAction
import capitalizationData


class RSICorridorAssessor(IAssessor[CandleSignal, ITurnBackAction]):
    _portfolio: IPortfolio
    _contextProvider: IContextProvider
    _thresholdCoef: float
    _upper_rsi: int
    _lower_rsi: int
    _profitCoef: float
    _stopCoef: float

    def __init__(
        self,
        portfolio: IPortfolio,
        contextProvider: IContextProvider,
        upper_rsi: int,
        lower_rsi: int,
        profitCoef: float,
        stopCoef: float
    ):
        self._portfolio = portfolio
        self._contextProvider = contextProvider
        self._upper_rsi = upper_rsi
        self._lower_rsi = lower_rsi
        self._profitCoef = profitCoef
        self._stopCoef = stopCoef

    def _calculate_rsi(self, prices: list[float], period: int = 14) -> float:
        if len(prices) <= period:
            return 50.0

        gains = []
        losses = []

        for i in range(1, len(prices)):
            diff = prices[i] - prices[i - 1]
            gains.append(max(0, diff))
            losses.append(max(0, -diff))

        avg_gain = sum(gains[:period]) / period
        avg_loss = sum(losses[:period]) / period

        for i in range(period, len(gains)):
            avg_gain = (avg_gain * (period - 1) + gains[i]) / period
            avg_loss = (avg_loss * (period - 1) + losses[i]) / period

        if avg_loss == 0:
            return 100.0

        rs = avg_gain / avg_loss
        return 100.0 - (100.0 / (1.0 + rs))

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

        prices = []
        for i in range(input.previousCandles.getCount()):
            candle = input.previousCandles.getByIndex(i)
            if candle:
                prices.append(candle.getOpenPrice())
        rsi_value = self._calculate_rsi(prices)
        lotToBuy = int((self._portfolio.getBaseAmount() * 0.4) / price)
        print("rsi:", rsi_value)

        multiplayer = 1.0
        atr = self.getATR(input)
        print("atr:", atr)
        if predictedPrice > price and rsi_value < self._upper_rsi:
            local_upper_rsi = self._upper_rsi - 10
            while local_upper_rsi > 0:
                if rsi_value < local_upper_rsi:
                    multiplayer += 0.125
                local_upper_rsi -= 10
            lotToBuy *= multiplayer
            lotToBuy = lotToBuy if lotToBuy > 0 else 0
            print("buying:", lotToBuy)
            highLimit = max(predictedPrice, price + atr * self._profitCoef) # 2.0
            lowLimit = price - (atr * self._stopCoef) # 1.2
            # highLimit = price + (atr * self._profitCoef)
            # lowLimit = price - (atr * self._stopCoef)
            return LimitBackAction(
                input,
                assetPair,
                int(lotToBuy),
                True,
                context,
                1,
                previousCandle.getOpenTimestamp(),
                nextCandle.getOpenTimestamp() - previousCandle.getOpenTimestamp(),
                highLimit,
                lowLimit
            )
        if predictedPrice < price and rsi_value > self._lower_rsi:
            local_lower_rsi = self._lower_rsi + 10
            while local_lower_rsi < 100:
                if rsi_value > local_lower_rsi:
                    multiplayer += 0.125
                local_lower_rsi += 10
            lotToBuy *= multiplayer
            lotToBuy = lotToBuy if lotToBuy > 0 else 0
            print("selling:", lotToBuy)
            highLimit = price + (atr * self._stopCoef)
            lowLimit = min(predictedPrice, price - atr * self._profitCoef)
            # highLimit = price + (atr * self._stopCoef)
            # lowLimit = price - (atr * self._profitCoef)
            return LimitBackAction(
                input,
                assetPair,
                int(lotToBuy),
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