from src.Interfaces import IAssetPair, IAsset

class AssetPair(IAssetPair):
    _baseAsset: IAsset
    _quoteAsset: IAsset

    def __init__(self, baseAsset: IAsset, quoteAsset: IAsset):
        if baseAsset == quoteAsset:
            raise ValueError(f"'baseAsset' must be different from 'quoteAsset', but 'baseAsset' is {baseAsset} and 'qouteAsset' is {quoteAsset}")

        self._baseAsset = baseAsset
        self._quoteAsset = quoteAsset

    def getBaseAsset(self) -> IAsset:
        return self._baseAsset

    def getQuoteAsset(self) -> IAsset:
        return self._quoteAsset

    def withBaseAsset(self, baseAsset) -> IAssetPair:
        if baseAsset == self._quoteAsset:
            raise ValueError(f"'baseAsset' must be different from 'quoteAsset', but 'baseAsset' is {baseAsset} and 'qouteAsset' is {self._quoteAsset}")
        return AssetPair(baseAsset, self._quoteAsset)

    def withQuoteAsset(self, quoteAsset) -> IAssetPair:
        if self._baseAsset == quoteAsset:
            raise ValueError(f"'baseAsset' must be different from 'quoteAsset', but 'baseAsset' is {self._baseAsset} and 'qouteAsset' is {quoteAsset}")
        return AssetPair(self._baseAsset, quoteAsset)

    def __eq__(self, other: IAssetPair) -> bool:
        if not isinstance(other, AssetPair):
            return False
        return (self._baseAsset == other.getBaseAsset() and
                self._quoteAsset == other.getQuoteAsset())

    def __hash__(self) -> int:
        return hash((self._baseAsset, self._quoteAsset))

    def __copy__(self) -> IAssetPair:
        return AssetPair(self._baseAsset, self._quoteAsset)

    def __str__(self) -> str:
        return f"{self._baseAsset} - {self._quoteAsset}"
