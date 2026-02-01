from Interfaces import IPredictor, ICandleSeries
from Entities import INextCandlePrediction, NextCandlePrediction
from ValueObjects import Candle

class DummyPredictor(IPredictor[INextCandlePrediction]):

    def predict(self, input: ICandleSeries) -> INextCandlePrediction:
        lastCandle = input.getByIndex(input.getCount() - 1)
        fromPrice = lastCandle.getClosePrice()
        toPrice = lastCandle.getClosePrice() + 10
        nextCandle = Candle(fromPrice, toPrice, fromPrice, toPrice)
        timestamp = lastCandle.getOpenTimestamp() + lastCandle.getInterval().value

        return NextCandlePrediction(timestamp, input, 0.5, nextCandle)
