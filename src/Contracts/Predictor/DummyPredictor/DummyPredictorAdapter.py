from Interfaces import IPredictorAdapter
from Entities import INextCandlePrediction, NextCandlePrediction
from ValueObjects import Candle
from .DummyPredictorValue import DummyPredictorValue

class DummyPredictorAdapter(IPredictorAdapter[DummyPredictorValue, INextCandlePrediction]):

    def transform(self, value: DummyPredictorValue) -> INextCandlePrediction:
        meta = value.meta
        candleSeries = meta.getCandleSeries()
        assetPair = candleSeries.getAssetPair()
        timestamp = meta.getTimestamp()
        lastCandle = candleSeries.getByIndex(candleSeries.getCount() - 1)

        if not lastCandle:
            raise ValueError("Cannot extract last candle from meta")

        interval = lastCandle.getInterval()
        openPrice = lastCandle.getOpenPrice()
        closePrice = lastCandle.getClosePrice()
        highPrice = lastCandle.getHighPrice()
        lowPrice = lastCandle.getLowPrice()
        volume = lastCandle.getVolume()

        nextCandle = Candle(assetPair, timestamp, interval, openPrice, closePrice, highPrice, lowPrice, volume)

        return NextCandlePrediction(meta, nextCandle)
