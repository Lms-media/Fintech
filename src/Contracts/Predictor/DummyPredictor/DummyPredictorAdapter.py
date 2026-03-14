from Interfaces import IPredictorAdapter
from Entities import INextCandlePrediction, NextCandlePrediction
from ValueObjects import Candle
from .DummyPredictorValue import DummyPredictorValue
import random

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
        openPrice = lastCandle.getOpenPrice() + 1
        closePrice = lastCandle.getClosePrice() + 1
        highPrice = max(openPrice, closePrice)
        lowPrice = min(openPrice, closePrice)
        volume = lastCandle.getVolume()

        nextCandle = Candle(assetPair, timestamp, interval, openPrice, closePrice, highPrice, lowPrice, volume)

        return NextCandlePrediction(meta, nextCandle)
