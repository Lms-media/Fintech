import sys
import os
import shutil

if os.path.exists('logs'):
    shutil.rmtree('logs')
os.makedirs('logs')

sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from config import assetPair
from UseCases import StrategyTestingUseCase
from Contracts import DirectPredictor
from Services import FileLogger
from Contracts import MoexCurrencyDataSource, LoggedDataSource
from Interfaces import IntervalType

predictor = DirectPredictor()
logger = FileLogger("logs/strategyTest.log")
dataSource = LoggedDataSource(MoexCurrencyDataSource(assetPair, "USD000UTSTOM", 0, 1701171835, IntervalType.OneDay), logger)

testingUseCase = StrategyTestingUseCase(dataSource, predictor, logger)
testingUseCase.execute()
