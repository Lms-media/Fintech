from Interfaces import IPredictorAdapter
from Entities import INextCandlePrediction, NextCandlePrediction
from ValueObjects import Candle
from .DummyPredictorValue import DummyPredictorValue

class DummyPredictorAdapter(IPredictorAdapter[DummyPredictorValue, INextCandlePrediction]):

    def transform(self, value: DummyPredictorValue) -> INextCandlePrediction:
        meta = value.meta
        priceDelta = value.priceDelta
        volume = value.volume
        candleSeries = meta.getCandleSeries()
        assetPair = candleSeries.getAssetPair()
        timestamp = meta.getTimestamp()
        lastCandle = candleSeries.getByIndex(candleSeries.getCount() - 1)
        interval = lastCandle.getInterval()
        fromPrice = lastCandle.getClosePrice()
        toPrice = lastCandle.getClosePrice() + priceDelta

        nextCandle = Candle(assetPair, timestamp, interval, fromPrice, toPrice, toPrice, fromPrice, volume)

        return NextCandlePrediction(meta, nextCandle)
