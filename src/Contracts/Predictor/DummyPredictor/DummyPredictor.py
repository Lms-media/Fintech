from Interfaces import IPredictor, ICandleSeries
from Entities import INextCandlePrediction, NextCandlePrediction
from ValueObjects import Candle

class DummyPredictor(IPredictor[INextCandlePrediction]):

    def predict(self, input: ICandleSeries) -> INextCandlePrediction:
        lastCandle = input.getByIndex(input.getCount() - 1)
        assetPair = input.getAssetPair()
        interval = lastCandle.getInterval()
        fromPrice = lastCandle.getClosePrice()
        toPrice = lastCandle.getClosePrice() + 10
        timestamp = lastCandle.getOpenTimestamp() + interval.value
        nextCandle = Candle(assetPair, timestamp, interval, fromPrice, toPrice, toPrice, fromPrice, 1)

        return NextCandlePrediction(timestamp, input, 0.5, nextCandle)
