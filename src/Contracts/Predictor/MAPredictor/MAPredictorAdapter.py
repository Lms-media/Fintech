from Interfaces import IPredictorAdapter
from Entities import INextCandlePrediction, NextCandlePrediction
from ValueObjects import Candle
from .MAPredictorValue import MAPredictorValue

class MAPredictorAdapter(IPredictorAdapter[MAPredictorValue, INextCandlePrediction]):

    def transform(self, value: MAPredictorValue) -> INextCandlePrediction:
        meta = value.meta
        intersectionPrice = value.intersectionPrice
        candleSeries = meta.getCandleSeries()
        lastCandle = candleSeries.getByIndex(candleSeries.getCount() - 1)
        priceDelta = intersectionPrice - lastCandle.getClosePrice()
        timestamp = meta.getTimestamp()
        interval = lastCandle.getInterval()
        openPrice = lastCandle.getClosePrice()
        assetPair = lastCandle.getAssetPair()
        closePrice = lastCandle.getClosePrice() + priceDelta / 2
        lowPrice = min(openPrice, closePrice)
        highPrice = max(openPrice, closePrice)
        volume = lastCandle.getVolume()

        nextCandle = Candle(assetPair, timestamp, interval, openPrice, closePrice, highPrice, lowPrice, volume)

        return NextCandlePrediction(meta, nextCandle)
