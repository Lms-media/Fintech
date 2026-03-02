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
from Services import FileLogger, LoggedPortfolio, RuntimePortfolio, PortfolioSyncMarket, LoggedMarket
from Contracts import MoexCurrencyDataSource, LoggedDataSource, DirectStrategy, CandleTestAssesor, LoggedContextProvider, DatasetContextProvider, BackgroundPollingExecutor, LoggedExecutor
from Interfaces import IntervalType

logger = FileLogger("logs/strategyTest.log")
datasourceLogger = FileLogger("logs/datasourceLogger.log")
portfolioLogger = FileLogger("logs/portfolioTest.log")
contextLogger = FileLogger("logs/contextTest.log")
executorContextLogger = FileLogger("logs/executorContextTest.log")
executorLogger = FileLogger("logs/executorLogger.log")
marketContextLogger = FileLogger("logs/marketContextTest.log")
marketLogger = FileLogger("logs/marketLogger.log")
portfolio = LoggedPortfolio(RuntimePortfolio(assetPair.getBaseAsset()), portfolioLogger)
portfolio.deposit(100000)
dataSource = LoggedDataSource(MoexCurrencyDataSource(assetPair, "USD000UTSTOM", 0, 1701171835, IntervalType.OneDay), datasourceLogger)
dataSource.init()
predictor = DirectPredictor(dataSource)
strategy = DirectStrategy()
contextProvider = LoggedContextProvider(DatasetContextProvider(assetPair, dataSource), contextLogger)
# executorContextProvider = LoggedContextProvider(DatasetContextProvider(assetPair, dataSource), executorContextLogger)
# marketContextProvider = LoggedContextProvider(DatasetContextProvider(assetPair, dataSource), marketContextLogger)
market = LoggedMarket(PortfolioSyncMarket(portfolio, contextProvider), marketLogger)
executor = LoggedExecutor(BackgroundPollingExecutor(market, contextProvider), executorLogger)
assessor = CandleTestAssesor(portfolio, contextProvider)

testingUseCase = StrategyTestingUseCase(dataSource, predictor, strategy, assessor, logger, executor)
testingUseCase.execute()
