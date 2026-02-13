import sys
import os
import shutil

if os.path.exists('logs'):
    shutil.rmtree('logs')
os.makedirs('logs')

sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from UseCases import PredictorTestingUseCase
from ValueObjects import Asset, AssetPair
from Contracts import MoexCurrencyDataSource, MAPredictor, LoggedDataSource, LoggedPredictor
from Services import FileLogger, GraphLogger2D, CompositeLogger, ParamLogger
from Interfaces import IntervalType

baseAsset = Asset('RUB', 1)
quoteAsset = Asset('USD', 10)
assetPair = AssetPair(baseAsset, quoteAsset)

dataSourceLogger = FileLogger("logs/dataSource.log")
predictorLogger = FileLogger("logs/predictor.log")
mainLogger = ParamLogger(CompositeLogger([FileLogger("logs/main.log"), GraphLogger2D("logs/main.png")]), 1, 1)
dataSource = LoggedDataSource(MoexCurrencyDataSource(assetPair, "USD000UTSTOM", 1676224135, 1707760135, IntervalType.OneDay), dataSourceLogger)
dataSource.init()

candleSeries = dataSource.getSeries()

useCase = PredictorTestingUseCase(candleSeries, mainLogger)
useCase.execute()
