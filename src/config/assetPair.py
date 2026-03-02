from ValueObjects import Asset, AssetPair

baseAsset = Asset('RUB', 1)
quoteAsset = Asset('USD', 1)
assetPair = AssetPair(baseAsset, quoteAsset)
