from Interfaces import ISignal, ICandle, ICandleSeries


class CandleSignal(ISignal):
    def __init__(self, timestamp: int, predictionCandle: ICandle, previousCandles: ICandleSeries):
        self._timestamp = timestamp
        self._predictionCandle = predictionCandle
        self.previousCandles = previousCandles

    def getTimestamp(self):
        return self._timestamp
    
    def getPrediction(self):
        return self._predictionCandle
    
    def getVolume(self):
        return self._predictionCandle.getVolume()
    
    def getId(self):
        return self._predictionCandle.getOpenTimestamp()
    
    def __str__(self):
        return self._predictionCandle.__str__()
