from Interfaces import IPredictorAdapter
from Entities import INextCandlePrediction, NextCandlePrediction
from ValueObjects import Candle
from .MLPredictorValue import MLPredictorValue

class MLPredictorAdapter(IPredictorAdapter[MLPredictorValue, INextCandlePrediction]):

    def transform(self, value: MLPredictorValue) -> INextCandlePrediction:
        meta = value.meta
        outputs = value.outputs
        limits = value.limits

        candleSeries = meta.getCandleSeries()
        assetPair = candleSeries.getAssetPair()
        lastCandle = candleSeries.getByIndex(candleSeries.getCount() - 1)

        if not lastCandle:
            raise ValueError(f"Failed to parse last candle from meta")

        timestamp = lastCandle.getOpenTimestamp()
        interval = lastCandle.getInterval()
        volume = 1

        openPrice = outputs[0] * (limits[0][1] - limits[0][0]) + limits[0][0]
        closePrice = outputs[1] * (limits[1][1] - limits[1][0]) + limits[1][0]
        highPrice = outputs[2] * (limits[2][1] - limits[2][0]) + limits[2][0]
        lowPrice = outputs[3] * (limits[3][1] - limits[3][0]) + limits[3][0]

        highPrice = max(openPrice, closePrice, highPrice, lowPrice)
        lowPrice = min(openPrice, closePrice, highPrice, lowPrice)

        nextCandle = Candle(assetPair, timestamp, interval, openPrice, closePrice, highPrice, lowPrice, volume)

        return NextCandlePrediction(meta, nextCandle)
