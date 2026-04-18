from Interfaces import IPredictorAdapter
from Entities import INextCandlePrediction, NextCandlePrediction
from ValueObjects import Candle
from .PercentageDeltaMLPredictorValue import PercentageDeltaMLPredictorValue

class PercentageDeltaMLPredictorAdapter(IPredictorAdapter[PercentageDeltaMLPredictorValue, INextCandlePrediction]):

    def transform(self, value: PercentageDeltaMLPredictorValue) -> INextCandlePrediction:
        meta = value.meta

        candleSeries = meta.getCandleSeries()
        assetPair = candleSeries.getAssetPair()
        lastCandle = candleSeries.getByIndex(candleSeries.getCount() - 1)

        if not lastCandle:
            raise ValueError(f"Failed to parse last candle from meta")

        pctOcDelta = value.ocDelta * value.ocDeltaFactor
        pctOhDelta = value.ohDelta * value.ohDeltaFactor
        pctOlDelta = value.olDelta * value.olDeltaFactor
        pctOffset = value.offset * value.offsetFactor

        interval = lastCandle.getInterval()
        timestamp = lastCandle.getOpenTimestamp() + interval.value
        volume = 1

        openPrice = lastCandle.getClosePrice() * (1 + pctOffset)
        closePrice = openPrice * (1 + pctOcDelta)
        highPrice = openPrice * (1 + pctOhDelta)
        lowPrice = openPrice * (1 + pctOlDelta)

        highPrice = max(openPrice, closePrice, highPrice, lowPrice)
        lowPrice = min(openPrice, closePrice, highPrice, lowPrice)

        nextCandle = Candle(assetPair, timestamp, interval, openPrice, closePrice, highPrice, lowPrice, volume)

        return NextCandlePrediction(meta, nextCandle)
