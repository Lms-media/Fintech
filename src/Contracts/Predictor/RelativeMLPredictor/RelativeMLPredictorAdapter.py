from Interfaces import IPredictorAdapter
from Entities import INextCandlePrediction, NextCandlePrediction
from ValueObjects import Candle
from .RelativeMLPredictorValue import RelativeMLPredictorValue

class RelativeMLPredictorAdapter(IPredictorAdapter[RelativeMLPredictorValue, INextCandlePrediction]):

    def transform(self, value: RelativeMLPredictorValue) -> INextCandlePrediction:
        meta = value.meta
        outputs = value.outputs
        maxAbsDelta = value.maxAbsDelta

        candleSeries = meta.getCandleSeries()
        assetPair = candleSeries.getAssetPair()
        lastCandle = candleSeries.getByIndex(candleSeries.getCount() - 1)

        if not lastCandle:
            raise ValueError(f"Failed to parse last candle from meta")

        interval = lastCandle.getInterval()
        timestamp = lastCandle.getOpenTimestamp() + interval.value
        volume = 1

        deltaOpenPrice = outputs[0] * maxAbsDelta[0]
        deltaClosePrice = outputs[1] * maxAbsDelta[1]
        deltaHighPrice = outputs[2] * maxAbsDelta[2]
        deltaLowPrice = outputs[3] * maxAbsDelta[3]

        openPrice = lastCandle.getOpenPrice() + deltaOpenPrice
        closePrice = lastCandle.getClosePrice() + deltaClosePrice
        highPrice = lastCandle.getHighPrice() + deltaHighPrice
        lowPrice = lastCandle.getLowPrice() + deltaLowPrice

        highPrice = max(openPrice, closePrice, highPrice, lowPrice)
        lowPrice = min(openPrice, closePrice, highPrice, lowPrice)

        nextCandle = Candle(assetPair, timestamp, interval, openPrice, closePrice, highPrice, lowPrice, volume)

        return NextCandlePrediction(meta, nextCandle)
