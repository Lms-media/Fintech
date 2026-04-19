from __future__ import annotations
from typing import Optional
from Interfaces import IExecutionContext, IAssetPair


class ExecutionContext(IExecutionContext):
    _timestamp: int
    _open_prices: dict[IAssetPair, float]
    _close_prices: dict[IAssetPair, float]
    _hight_prices: dict[IAssetPair, float]
    _low_prices: dict[IAssetPair, float]

    def __init__(
        self,
        timestamp: int,
        openPrices: dict[IAssetPair, float],
        closePrices: dict[IAssetPair, float],
        hightPrices: dict[IAssetPair, float],
        lowPrices: dict[IAssetPair, float],
    ):
        if timestamp < 0:
            raise ValueError(
                f"'timestamp' must be greater than or equal zero, but 'timestamp' is {timestamp}"
            )

        self._timestamp = timestamp
        self._open_prices = dict(openPrices)
        self._close_prices = dict(closePrices)
        self._hight_prices = dict(hightPrices)
        self._low_prices = dict(lowPrices)

    def getPrice(self, assetPair: IAssetPair) -> Optional[float]:
        if assetPair in self._open_prices:
            return self._open_prices[assetPair]

        return None

    def getClosePrice(self, assetPair: IAssetPair) -> Optional[float]:
        if assetPair in self._close_prices:
            return self._close_prices[assetPair]

        return None

    def getHightPrice(self, assetPair: IAssetPair) -> Optional[float]:
        if assetPair in self._hight_prices:
            return self._hight_prices[assetPair]

        return None

    def getLowPrice(self, assetPair: IAssetPair) -> Optional[float]:
        if assetPair in self._low_prices:
            return self._low_prices[assetPair]

        return None

    def getTimestamp(self) -> int:
        return self._timestamp

    def withPrice(
        self,
        assetPair: IAssetPair,
        openPrice: float,
        closePrice: float,
        hightPrice: float,
        lowPrice: float,
    ) -> IExecutionContext:
        newOpenPrices = dict(self._open_prices)
        newOpenPrices[assetPair] = openPrice

        newClosePrices = dict(self._close_prices)
        newClosePrices[assetPair] = closePrice

        newHightPrices = dict(self._hight_prices)
        newHightPrices[assetPair] = hightPrice

        newLowPrices = dict(self._low_prices)
        newLowPrices[assetPair] = lowPrice

        return ExecutionContext(
            self._timestamp, newOpenPrices, newClosePrices, newHightPrices, newLowPrices
        )

    def withTimestamp(self, timestamp: int) -> IExecutionContext:
        if timestamp < 0:
            raise ValueError(
                f"'timestamp' must be greater than or equal zero, but 'timestamp' is {timestamp}"
            )

        return ExecutionContext(
            timestamp,
            self._open_prices,
            self._close_prices,
            self._hight_prices,
            self._low_prices,
        )

    def __eq__(self, other) -> bool:
        if not isinstance(other, ExecutionContext):
            return False

        return (
            self._timestamp == other._timestamp
            and self._open_prices == other._open_prices
        )

    def __hash__(self) -> int:
        pricesHashable = frozenset((k, v) for k, v in self._open_prices.items())
        return hash((self._timestamp, pricesHashable))

    def __copy__(self) -> IExecutionContext:
        return ExecutionContext(
            self._timestamp,
            dict(self._open_prices),
            dict(self._close_prices),
            dict(self._hight_prices),
            dict(self._low_prices),
        )

    def __str__(self) -> str:
        result = [f"ℹ️ Timestamp: {self._timestamp}"]

        for assetPair, price in self._open_prices.items():
            result.append(f"{assetPair} - {price:.4f}")

        return "\n".join(result)
