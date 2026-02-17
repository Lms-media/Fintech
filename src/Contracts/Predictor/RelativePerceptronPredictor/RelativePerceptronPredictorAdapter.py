from Interfaces import IPredictorAdapter
from Entities import INextCandlePrediction, NextCandlePrediction
from ValueObjects import Candle
from .RelativePerceptronPredictorValue import RelativePerceptronPredictorValue

class RelativePerceptronPredictorAdapter(IPredictorAdapter[RelativePerceptronPredictorValue, INextCandlePrediction]):

    def transform(self, value: RelativePerceptronPredictorValue) -> INextCandlePrediction:
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

        deltaOpenPrice = outputs[0] * (limits[0][1] - limits[0][0]) + limits[0][0]
        deltaClosePrice = outputs[1] * (limits[1][1] - limits[1][0]) + limits[1][0]
        deltaHighPrice = outputs[2] * (limits[2][1] - limits[2][0]) + limits[2][0]
        dletaLowPrice = outputs[3] * (limits[3][1] - limits[3][0]) + limits[3][0]

        openPrice = lastCandle.getOpenPrice() + deltaOpenPrice
        closePrice = lastCandle.getClosePrice() + deltaClosePrice
        highPrice = lastCandle.getHighPrice() + deltaHighPrice
        lowPrice = lastCandle.getLowPrice() + dletaLowPrice

        highPrice = max(openPrice, closePrice, highPrice, lowPrice)
        lowPrice = min(openPrice, closePrice, highPrice, lowPrice)

        nextCandle = Candle(assetPair, timestamp, interval, openPrice, closePrice, highPrice, lowPrice, volume)

        return NextCandlePrediction(meta, nextCandle)
