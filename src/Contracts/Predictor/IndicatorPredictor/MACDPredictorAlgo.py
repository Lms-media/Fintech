from Interfaces import IPredictorAlgo, ICandleSeries
from .IndicatorPredictorValue import IndicatorPredictorValue
from Entities import PredictionMeta

class MACDPredictorAlgo(IPredictorAlgo[IndicatorPredictorValue]):
    _fastPeriod: int
    _slowPeriod: int
    _signalPeriod: int

    def __init__(self, candlesCount: int):
        self._slowPeriod = candlesCount
        self._fastPeriod = max(round(candlesCount / 2), 1)
        self._signalPeriod = max(round(candlesCount / 3), 1)

    def calc(self, input: ICandleSeries):
        lastCandle = input.getByIndex(input.getCount() - 1)

        if not lastCandle:
            raise ValueError("Cannot extract last candle from meta")

        interval = lastCandle.getInterval()
        timestamp = lastCandle.getOpenTimestamp() + interval.value
        meta = PredictionMeta(timestamp, input, 0.5)

        closes = []
        deltaSum = 0
        for i in range(input.getCount()):
            candle = input.getByIndex(input.getCount() - 1 - i)
            if candle:
                closes.insert(0, candle.getClosePrice())
                deltaSum += abs(candle.getOpenPrice() - candle.getClosePrice())

        if len(closes) < 2:
            return IndicatorPredictorValue(meta, 0)

        slowPeriod = min(self._slowPeriod, len(closes))
        fastPeriod = min(self._fastPeriod, slowPeriod)
        signalPeriod = min(self._signalPeriod, slowPeriod)

        fastEMA = self._calcEMA(closes, fastPeriod)
        slowEMA = self._calcEMA(closes, slowPeriod)

        macdLine = []
        for i in range(max(len(closes) - slowPeriod + 1, 1)):
            subset = closes[:slowPeriod + i] if slowPeriod + i <= len(closes) else closes
            fast = self._calcEMA(subset, fastPeriod)
            slow = self._calcEMA(subset, slowPeriod)
            macdLine.append(fast - slow)

        if len(macdLine) < 1:
            return IndicatorPredictorValue(meta, 0)

        signalLine = self._calcEMA(macdLine, min(signalPeriod, len(macdLine)))
        currentMACD = fastEMA - slowEMA
        histogram = currentMACD - signalLine

        avgDelta = deltaSum / len(closes) if closes else 1

        if histogram > 0 and currentMACD > 0:
            return IndicatorPredictorValue(meta, avgDelta)
        elif histogram < 0 and currentMACD < 0:
            return IndicatorPredictorValue(meta, -avgDelta)
        elif histogram > 0:
            return IndicatorPredictorValue(meta, avgDelta * 0.5)
        elif histogram < 0:
            return IndicatorPredictorValue(meta, -avgDelta * 0.5)
        else:
            return IndicatorPredictorValue(meta, 0)

    def _calcEMA(self, data: list[float], period: int) -> float:
        if len(data) < period:
            return sum(data) / len(data) if data else 0

        alpha = 2.0 / (period + 1)
        ema = sum(data[:period]) / period

        for i in range(period, len(data)):
            ema = alpha * data[i] + (1 - alpha) * ema

        return ema
