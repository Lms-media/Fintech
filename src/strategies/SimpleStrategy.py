from src.strategies.Strategy import Strategy
from src.data.Candle import Candle

class SimpleStrategy(Strategy):
    def __init__(self, chunkSize: int):
        self.chunkSize = chunkSize
        
    def predict(self, chunk: list[Candle]) ->  float:
        reversedCandles = chunk[::-1]
        stableCount = 1
        diff = reversedCandles[0].close - reversedCandles[1].close
        previous = reversedCandles[1]
        current = reversedCandles[2]
        i = 3
        while diff * (previous.close - current.close) > 0:
            stableCount += 1
            previous = current
            current = reversedCandles[i]
            i += 1
        close = 0
        for j in range(stableCount):
            close += reversedCandles[j].close - reversedCandles[j + 1].close

        del2  = close / stableCount
        close = reversedCandles[0].close
        sign = 1 if (j // stableCount) % 2 == 0 else -1
        close += del2 * sign
        
        return 1 if chunk[-1].close > close else -1