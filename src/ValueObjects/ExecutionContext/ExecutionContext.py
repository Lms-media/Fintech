from __future__ import annotations
from typing import Optional
from Interfaces import IExecutionContext, IAssetPair

class ExecutionContext(IExecutionContext):
    _timestamp: int
    _prices: dict[IAssetPair, float]

    def __init__(self, timestamp: int, prices: dict[IAssetPair, float]):
        if timestamp < 0:
            raise ValueError(f"'timestamp' must be greater than or equal zero, but 'timestamp' is {timestamp}")

        self._timestamp = timestamp
        self._prices = dict(prices)

    def getPrice(self, assetPair: IAssetPair) -> Optional[float]:
        if assetPair in self._prices:
            return self._prices[assetPair]

        return None

    def getTimestamp(self) -> int:
        return self._timestamp

    def withPrice(self, assetPair: IAssetPair, price: float) -> IExecutionContext:
        newPrices = dict(self._prices)
        newPrices[assetPair] = price

        return ExecutionContext(self._timestamp, newPrices)

    def withTimestamp(self, timestamp: int) -> IExecutionContext:
        if timestamp < 0:
            raise ValueError(f"'timestamp' must be greater than or equal zero, but 'timestamp' is {timestamp}")

        return ExecutionContext(timestamp, self._prices)

    def __eq__(self, other) -> bool:
        if not isinstance(other, ExecutionContext):
            return False

        return self._timestamp == other._timestamp and self._prices == other._prices

    def __hash__(self) -> int:
        pricesHashable = frozenset((k, v) for k, v in self._prices.items())
        return hash((self._timestamp, pricesHashable))

    def __copy__(self) -> IExecutionContext:
        return ExecutionContext(self._timestamp, dict(self._prices))

    def __str__(self) -> str:
        result = [f"ℹ️ Timestamp: {self._timestamp}"]

        for assetPair, price in self._prices.items():
            result.append(f"{assetPair} - {price:.4f}")

        return "\n".join(result)
