import sys
import os
import shutil

if os.path.exists('logs'):
    shutil.rmtree('logs')
os.makedirs('logs')

sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from Factories import DummyFactory
from UseCases import InfiniteActionUseCase
from ValueObjects import Asset, AssetPair

baseAsset = Asset('RUB', 1)
quoteAsset = Asset('USD', 10)
assetPair = AssetPair(baseAsset, quoteAsset)

factory = DummyFactory(assetPair)
useCase = InfiniteActionUseCase(factory, 2)

useCase.execute()
