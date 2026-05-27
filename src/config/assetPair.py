from ValueObjects import Asset, AssetPair

baseAsset = Asset('RUB', 1)
quoteAsset = Asset('USD', 10)
assetPair = AssetPair(baseAsset, quoteAsset)
