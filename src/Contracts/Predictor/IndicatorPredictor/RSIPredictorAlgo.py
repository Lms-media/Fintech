from Interfaces import IPredictorAlgo, ICandleSeries
from .IndicatorPredictorValue import IndicatorPredictorValue
from Entities import PredictionMeta

class RSIPredictorAlgo(IPredictorAlgo[IndicatorPredictorValue]):
    _candlesCount: int
    _overboughtLevel: float
    _oversoldLevel: float
    _neutralLevel: float

    def __init__(self, candlesCount: int, overboughtLevel: float = 70, oversoldLevel: float = 30, neutralLevel: float = 50):
        self._candlesCount = candlesCount
        self._overboughtLevel = overboughtLevel
        self._oversoldLevel = oversoldLevel
        self._neutralLevel = neutralLevel

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

        gains = []
        losses = []
        for i in range(1, len(closes)):
            change = closes[i] - closes[i - 1]
            if change > 0:
                gains.append(change)
                losses.append(0)
            else:
                gains.append(0)
                losses.append(abs(change))

        period = min(self._candlesCount, len(gains))
        avgGain = sum(gains[-period:]) / period
        avgLoss = sum(losses[-period:]) / period

        if avgLoss == 0:
            rsi = 100
        else:
            rs = avgGain / avgLoss
            rsi = 100 - (100 / (1 + rs))

        avgDelta = deltaSum / len(closes) if closes else 1

        if rsi >= self._overboughtLevel:
            return IndicatorPredictorValue(meta, -avgDelta)
        elif rsi <= self._oversoldLevel:
            return IndicatorPredictorValue(meta, avgDelta)
        elif rsi > self._neutralLevel:
            strength = (rsi - self._neutralLevel) / (self._overboughtLevel - self._neutralLevel)
            return IndicatorPredictorValue(meta, -avgDelta * strength)
        elif rsi < self._neutralLevel:
            strength = (self._neutralLevel - rsi) / (self._neutralLevel - self._oversoldLevel)
            return IndicatorPredictorValue(meta, avgDelta * strength)
        else:
            return IndicatorPredictorValue(meta, 0)
