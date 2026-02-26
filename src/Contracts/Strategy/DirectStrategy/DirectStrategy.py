from Interfaces import IStrategy, DirectionType
from Entities import INextCandlePrediction, CandleSignal

class DirectStrategy(IStrategy[INextCandlePrediction, CandleSignal]):
    def getSignal(self, input: INextCandlePrediction) -> CandleSignal:
        predictionCandles = input.getMeta().getCandleSeries()
        nextCandle = input.getNextCandle()
        timestamp = nextCandle.getOpenPrice()
        return CandleSignal(int(timestamp), nextCandle, predictionCandles)
