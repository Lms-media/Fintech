import math
from Interfaces import IPredictorAlgo, ICandleSeries
from .IndicatorPredictorValue import IndicatorPredictorValue
from Entities import PredictionMeta

class BollingerBandsPredictorAlgo(IPredictorAlgo[IndicatorPredictorValue]):
    _candlesCount: int
    _stdDevMultiplier: float

    def __init__(self, candlesCount: int = 20, stdDevMultiplier: float = 2.0):
        self._candlesCount = candlesCount
        self._stdDevMultiplier = stdDevMultiplier

    def calc(self, input: ICandleSeries):
        lastCandle = input.getByIndex(input.getCount() - 1)

        if not lastCandle:
            raise ValueError("Cannot extract last candle from meta")

        interval = lastCandle.getInterval()
        timestamp = lastCandle.getOpenTimestamp() + interval.value
        meta = PredictionMeta(timestamp, input, 0.5)

        closes = []
        deltaSum = 0
        for i in range(min(self._candlesCount, input.getCount())):
            candle = input.getByIndex(input.getCount() - 1 - i)
            if candle:
                closes.insert(0, candle.getClosePrice())
                deltaSum += abs(candle.getOpenPrice() - candle.getClosePrice())

        if len(closes) < self._candlesCount:
            return IndicatorPredictorValue(meta, 0)

        sma = sum(closes) / len(closes)

        variance = sum((x - sma) ** 2 for x in closes) / len(closes)
        stdDev = math.sqrt(variance)

        upperBand = sma + (self._stdDevMultiplier * stdDev)
        lowerBand = sma - (self._stdDevMultiplier * stdDev)

        currentPrice = lastCandle.getClosePrice()
        avgDelta = deltaSum / len(closes) if closes else 1

        bandWidth = upperBand - lowerBand
        if bandWidth == 0:
            return IndicatorPredictorValue(meta, 0)

        percentB = (currentPrice - lowerBand) / bandWidth

        if currentPrice >= upperBand:
            return IndicatorPredictorValue(meta, -avgDelta)
        elif currentPrice <= lowerBand:
            return IndicatorPredictorValue(meta, avgDelta)
        elif percentB > 0.8:
            strength = (percentB - 0.8) / 0.2
            return IndicatorPredictorValue(meta, -avgDelta * strength)
        elif percentB < 0.2:
            strength = (0.2 - percentB) / 0.2
            return IndicatorPredictorValue(meta, avgDelta * strength)
        elif currentPrice > sma:
            return IndicatorPredictorValue(meta, avgDelta * 0.3)
        elif currentPrice < sma:
            return IndicatorPredictorValue(meta, -avgDelta * 0.3)
        else:
            return IndicatorPredictorValue(meta, 0)
