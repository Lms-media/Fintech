from Interfaces import IPredictorAdapter
from Entities import INextCandlePrediction, NextCandlePrediction
from ValueObjects import Candle
from .PercentageMLPredictorValue import PercentageMLPredictorValue

class PercentageMLPredictorAdapter(IPredictorAdapter[PercentageMLPredictorValue, INextCandlePrediction]):

    def transform(self, value: PercentageMLPredictorValue) -> INextCandlePrediction:
        meta = value.meta
        outputs = value.outputs
        maxAbsPct = value.maxAbsPct

        candleSeries = meta.getCandleSeries()
        assetPair = candleSeries.getAssetPair()
        lastCandle = candleSeries.getByIndex(candleSeries.getCount() - 1)

        if not lastCandle:
            raise ValueError(f"Failed to parse last candle from meta")

        interval = lastCandle.getInterval()
        timestamp = lastCandle.getOpenTimestamp() + interval.value
        volume = 1

        pctOpenPrice = outputs[0] * maxAbsPct[0]
        pctClosePrice = outputs[1] * maxAbsPct[1]
        pctHighPrice = outputs[2] * maxAbsPct[2]
        pctLowPrice = outputs[3] * maxAbsPct[3]

        openPrice = lastCandle.getOpenPrice() * (1 + pctOpenPrice)
        closePrice = lastCandle.getClosePrice() * (1 + pctClosePrice)
        highPrice = lastCandle.getHighPrice() * (1 + pctHighPrice)
        lowPrice = lastCandle.getLowPrice() * (1 + pctLowPrice)

        highPrice = max(openPrice, closePrice, highPrice, lowPrice)
        lowPrice = min(openPrice, closePrice, highPrice, lowPrice)

        nextCandle = Candle(assetPair, timestamp, interval, openPrice, closePrice, highPrice, lowPrice, volume)

        return NextCandlePrediction(meta, nextCandle)
