from Interfaces import IPredictorAdapter
from Entities import INextCandlePrediction, NextCandlePrediction
from ValueObjects import Candle
from .MAPredictorValue import MAPredictorValue

class MAPredictorAdapter(IPredictorAdapter[MAPredictorValue, INextCandlePrediction]):
    _sensitivity: float

    def __init__(self, sensitivity: float):
        self._sensitivity = sensitivity

    def transform(self, value: MAPredictorValue) -> INextCandlePrediction:
        meta = value.meta
        priceDelta = value.priceDelta
        candleSeries = meta.getCandleSeries()
        lastCandle = candleSeries.getByIndex(candleSeries.getCount() - 1)
        timestamp = meta.getTimestamp()
        interval = lastCandle.getInterval()
        openPrice = lastCandle.getClosePrice()
        assetPair = lastCandle.getAssetPair()
        closePrice = lastCandle.getClosePrice() - priceDelta
        lowPrice = min(openPrice, closePrice)
        highPrice = max(openPrice, closePrice)
        volume = lastCandle.getVolume()

        nextCandle = Candle(assetPair, timestamp, interval, openPrice, closePrice, highPrice, lowPrice, volume)

        return NextCandlePrediction(meta, nextCandle)
