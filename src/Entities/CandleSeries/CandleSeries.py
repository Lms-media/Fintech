from typing import Optional, Deque
from collections import deque
import uuid
from Interfaces import ICandleSeries, ICandle, IAssetPair

class CandleSeries(ICandleSeries):
    _id: str
    _candles: Deque[ICandle]
    _assetPair: IAssetPair

    def __init__(self, assetPair: IAssetPair):
        self._candles = deque()
        self._assetPair = assetPair
        self._id = uuid.uuid4()

    def getId(self) -> str:
        return self._id

    def getCount(self) -> int:
        return len(self._candles)

    def getByIndex(self, index: int) -> Optional[ICandle]:
        if 0 <= index < len(self._candles):
            return self._candles[index]

        return None

    def getAssetPair(self) -> IAssetPair:
        return  self._assetPair

    def getByTimestamp(self, timestamp: int) -> Optional[ICandle]:
        index = self._locate(timestamp)
        if index >= 0:
            return self._candles[index]

        return None

    def appendLeft(self, candle: ICandle) -> None:
        if not candle.getAssetPair() == self._assetPair:
            raise ValueError(f"Candle asset pair must be equal to candle series asset pair, but candle series asset pair is {self._assetPair} and candle asset pair is {candle.getAssetPair()}")

        if not self._candles:
            self._candles.appendleft(candle)
            return None

        firstCandle = self._candles[0]
        newEndTimestamp = candle.getOpenTimestamp() + candle.getInterval().value

        if not newEndTimestamp > firstCandle.getOpenTimestamp():
            raise ValueError(f"New candle must adjoin the first candle on {firstCandle.getOpenTimestamp()}, but actual end of new candle is {newEndTimestamp}")

        self._candles.appendleft(candle)

    def appendRight(self, candle: ICandle) -> None:
        if not candle.getAssetPair() == self._assetPair:
            raise ValueError(f"Candle asset pair must be equal to candle series asset pair, but candle series asset pair is {self._assetPair} and candle asset pair is {candle.getAssetPair()}")

        if not self._candles:
            self._candles.append(candle)
            return None

        lastCandle = self._candles[-1]
        endTimestamp = lastCandle.getOpenTimestamp() + lastCandle.getInterval().value

        if candle.getOpenTimestamp() < endTimestamp:
            raise ValueError(f"New candle must adjoin the last candle on {endTimestamp}, but actual start of new candle is {candle.getOpenTimestamp()}")

        self._candles.append(candle)

    def popLeft(self) -> Optional[ICandle]:
        if not self._candles:
            return None

        return self._candles.popleft()

    def popRight(self) -> Optional[ICandle]:
        if not self._candles:
            return None

        return self._candles.pop()

    def __str__(self) -> str:
        lines = list([f'📋 ({self.getId()})'])

        for i in range(self.getCount()):
            candle = self.getByIndex(i)
            lines.append(f"{i + 1}. {candle}")

        return '\n'.join(lines)

    def _locate(self, timestamp: int) -> int:
        if not self._candles:
            return -1

        first_candle = self._candles[0]
        if timestamp < first_candle.getOpenTimestamp():
            return -1

        last_candle = self._candles[-1]
        if timestamp >= last_candle.getOpenTimestamp():
            return len(self._candles) - 1

        left = 0
        right = len(self._candles) - 1

        while left <= right:
            mid = (left + right) // 2
            candle = self._candles[mid]
            start_time = candle.getOpenTimestamp()

            if start_time <= timestamp:
                if mid == len(self._candles) - 1:
                    return mid
                next_candle = self._candles[mid + 1]
                if next_candle.getOpenTimestamp() > timestamp:
                    return mid
                else:
                    left = mid + 1
            else:
                right = mid - 1

        return -1
