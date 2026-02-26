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
from Services import FileLogger, LoggedPortfolio, RuntimePortfolio
from Contracts import MoexCurrencyDataSource, LoggedDataSource, DirectStrategy, CandleTestAssesor, MockContextProvider, LoggedContextProvider
from Interfaces import IntervalType

logger = FileLogger("logs/strategyTest.log")
portfolioLogger = FileLogger("logs/portfolioTest.log")
contextLogger = FileLogger("logs/contextTest.log")
portfolio = LoggedPortfolio(RuntimePortfolio(assetPair.getBaseAsset()), portfolioLogger)
dataSource = LoggedDataSource(MoexCurrencyDataSource(assetPair, "USD000UTSTOM", 0, 1701171835, IntervalType.OneDay), logger)
dataSource.init()
predictor = DirectPredictor(dataSource)
strategy = DirectStrategy()
contextProvider = LoggedContextProvider(MockContextProvider(assetPair), contextLogger)
assessor = CandleTestAssesor(portfolio, contextProvider)

testingUseCase = StrategyTestingUseCase(dataSource, predictor, strategy, assessor, logger)
testingUseCase.execute()
