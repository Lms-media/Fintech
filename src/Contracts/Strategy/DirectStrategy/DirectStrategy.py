from Interfaces import IStrategy, DirectionType
from Entities import INextCandlePrediction, CandleSignal

class DirectStrategy(IStrategy[INextCandlePrediction, CandleSignal]):
    def getSignal(self, input: INextCandlePrediction) -> CandleSignal:
        predictionCandles = input.getMeta().getCandleSeries()
        nextCandle = input.getNextCandle()
        timestamp = predictionCandles.getByIndex(predictionCandles.getCount() - 1).getOpenTimestamp()
        return CandleSignal(timestamp, nextCandle, predictionCandles)
