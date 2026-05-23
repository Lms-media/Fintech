from ValueObjects import Asset, AssetPair

baseAsset = Asset('RUB', 1)
# KZT
# TRY
# USD
quoteAsset = Asset('KZT', 1)
assetPair = AssetPair(baseAsset, quoteAsset)
