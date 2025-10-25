class MLModule:
    @staticmethod
    def getNextCandle(candles: list, candlesCount: int = 1):
        reversedCandles = candles[::-1]
        stableCount = 1
        diff = reversedCandles[0]['close'] - reversedCandles[1]['close']
        previous = reversedCandles[1]
        current = reversedCandles[2]
        i = 3
        while diff * (previous['close'] - current['close']) > 0:
            stableCount += 1
            previous = current
            current = reversedCandles[i]
            i += 1
        open = 0
        close = 0
        high = 0
        low = 0
        for j in range(stableCount):
            open += reversedCandles[j]['open'] - reversedCandles[j + 1]['open']
            close += reversedCandles[j]['close'] - reversedCandles[j + 1]['close']
            high += reversedCandles[j]['high'] - reversedCandles[j + 1]['high']
            low += reversedCandles[j]['low'] - reversedCandles[j + 1]['low']

        del1 = open / stableCount
        del2  = close / stableCount
        del3 = high / stableCount
        del4 = low / stableCount
        open = reversedCandles[0]['open']
        close = reversedCandles[0]['close']
        high = reversedCandles[0]['high']
        low = reversedCandles[0]['low']
        outCandles = []
        for j in range(candlesCount):
            sign = 1 if (j // stableCount) % 2 == 0 else -1
            open += del1 * sign
            close += del2 * sign
            high += del3 * sign
            low += del4 * sign
            outCandles.append({'open' : round(open, 4), 'close' : round(close, 4), 'high' : round(high, 4), 'low' : round(low, 4)})

        return outCandles