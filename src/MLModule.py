class MLModule:
    @staticmethod
    def getNextCandle(candles: list):
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
        print(stableCount)
        open = 0
        close = 0
        high = 0
        low = 0
        for j in range(stableCount):
            open += reversedCandles[j]['open'] - reversedCandles[j + 1]['open']
            close += reversedCandles[j]['close'] - reversedCandles[j + 1]['close']
            high += reversedCandles[j]['high'] - reversedCandles[j + 1]['high']
            low += reversedCandles[j]['low'] - reversedCandles[j + 1]['low']
        open = round(reversedCandles[0]['open'] + open / stableCount, 4)
        close = round(reversedCandles[0]['close'] + close / stableCount, 4)
        high = round(reversedCandles[0]['high'] + high / stableCount, 4)
        low = round(reversedCandles[0]['low'] + low / stableCount, 4)

        return {'open' : open, 'close' : close, 'high' : high, 'low' : low}