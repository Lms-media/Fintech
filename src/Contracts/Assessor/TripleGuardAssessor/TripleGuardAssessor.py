from Interfaces import IAssessor
from Entities import CandleSignal, ITurnBackAction, TurnBackAction, LimitBackAction
import capitalizationData


class TripleGuardAssessor(IAssessor[CandleSignal, ITurnBackAction]):
    def __init__(self, portfolio, contextProvider, upper_rsi=70, lower_rsi=30):
        self._portfolio = portfolio
        self._contextProvider = contextProvider
        self._upper_rsi = upper_rsi
        self._lower_rsi = lower_rsi
        self._prev_predictions = []

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

    def _calculate_atr(self, previousCandles, period: int = 14) -> float:
        count = previousCandles.getCount()
        if count <= period:
            return 0.0

        true_ranges = []
        for i in range(1, count):
            candle = previousCandles.getByIndex(i)
            prev_candle = previousCandles.getByIndex(i - 1)

            if not candle or not prev_candle:
                continue

            hl = candle.getHighPrice() - candle.getLowPrice()
            hc = abs(candle.getHighPrice() - prev_candle.getClosePrice())
            lc = abs(candle.getLowPrice() - prev_candle.getClosePrice())

            true_ranges.append(max(hl, hc, lc))

        if not true_ranges:
            return 0.0
        
        atr = sum(true_ranges[:period]) / period

        for i in range(period, len(true_ranges)):
            atr = (atr * (period - 1) + true_ranges[i]) / period

        return atr

    def getAction(self, input: CandleSignal) -> ITurnBackAction:
        prediction = input.getPrediction()
        predictedPrice = prediction.getClosePrice()
        prevCandle = input.previousCandles.getByIndex(
            input.previousCandles.getCount() - 1
        )
        nextCandle = self._contextProvider.getNextCandle(prevCandle)
        if not nextCandle:
            raise ValueError("Invalid next candle")

        if not prevCandle:
            raise ValueError("Invalid candle")

        context = self._contextProvider.getContext(prevCandle.getOpenTimestamp())
        price = context.getClosePrice(input.previousCandles.getAssetPair())

        prices = []
        for i in range(input.previousCandles.getCount()):
            candle = input.previousCandles.getByIndex(i)
            if candle:
                prices.append(candle.getClosePrice())

        rsi_val = self._calculate_rsi(prices)
        atr = self._calculate_atr(input.previousCandles)
        sma = sum(prices) / len(prices)

        self._prev_predictions.append(predictedPrice)
        if len(self._prev_predictions) > 5:
            self._prev_predictions.pop(0)
        avg_pred = sum(self._prev_predictions) / len(self._prev_predictions)

        diff = predictedPrice - price
        is_significant = abs(diff) > (atr * 1.5)

        buy_signal = (
            price > sma
            and rsi_val < self._upper_rsi
            and predictedPrice > price
            and predictedPrice > avg_pred
        )

        sell_signal = (
            price < sma
            and rsi_val > self._lower_rsi
            and predictedPrice < price
            and predictedPrice < avg_pred
        )

        print(self._portfolio.getCapitalization(context))
        capitalizationData.cap.append(self._portfolio.getCapitalization(context))
        capitalizationData.actual.append(
            (nextCandle.getOpenTimestamp(), nextCandle.getClosePrice())
        )
        capitalizationData.predictions.append(
            (nextCandle.getOpenTimestamp(), prediction.getClosePrice())
        )
        print(price)
        print("predicted:", predictedPrice)
        print("rsi:", rsi_val)

        if (buy_signal or sell_signal):
            if buy_signal:
                hLimit = price + (atr * 2.5)
                lLimit = price - (atr * 1.0)
                direction = True
            else:
                hLimit = price + (atr * 1.0)
                lLimit = price - (atr * 2.5)
                direction = False

            lot = int((self._portfolio.getBaseAmount()) / price)
            if direction:
                print("buying:", lot)
            else:
                print("selling:", lot)

            return LimitBackAction(
                input,
                input.previousCandles.getAssetPair(),
                lot,
                direction,
                context,
                1,
                prevCandle.getOpenTimestamp(),
                nextCandle.getOpenTimestamp() - prevCandle.getOpenTimestamp(),
                hightLimit=hLimit,
                lowLimit=lLimit,
            )

        print("skipping")
        return TurnBackAction(
            input,
            input.previousCandles.getAssetPair(),
            0,
            True,
            context,
            1,
            prevCandle.getOpenTimestamp(),
            nextCandle.getOpenTimestamp() - prevCandle.getOpenTimestamp(),
        )
