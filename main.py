import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from Factories import DummyFactory
from UseCases import SingleActionUseCase
from ValueObjects import Asset, AssetPair

baseAsset = Asset('RUB', 1)
quoteAsset = Asset('USD', 10)
assetPair = AssetPair(baseAsset, quoteAsset)

factory = DummyFactory(assetPair)
useCase = SingleActionUseCase(factory)

useCase.execute()
