import sys
import os
import shutil
import argparse
import json

if os.path.exists('logs'):
    shutil.rmtree('logs')
os.makedirs('logs')

sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from config import assetPair
from UseCases import StrategyTestingUseCase, PredictorTrainingUseCase
from Contracts import DirectPredictor, PercentageMLPredictor, IndicatorPredictor
from Services import FileLogger, LoggedPortfolio, RuntimePortfolio, PortfolioSyncMarket, LoggedMarket
from Contracts import MoexCurrencyDataSource, LoggedDataSource, DirectStrategy, CandleTestAssesor, TrendFilterAssessor, VolatilityThresholdAssessor, RSIFilterAssessor, LoggedContextProvider, DatasetContextProvider, BackgroundPollingExecutor, LoggedExecutor
from Interfaces import IntervalType

parser = argparse.ArgumentParser()
parser.add_argument('--config', type=str)
config_path = parser.parse_args().config

with open(config_path, 'r', encoding='utf-8') as file:
            config = json.load(file)

logger = FileLogger("logs/strategyTest.log")
trainingUseCaseLogger = FileLogger("logs/trainingUseCase.log")
portfolio = RuntimePortfolio(assetPair.getBaseAsset())
portfolio.deposit(100000)
print("datasource init...")
dataSource = MoexCurrencyDataSource(assetPair, "USD000UTSTOM", 0, 1701171835, IntervalType.OneDay)
dataSource.init()
trainingDataSource = MoexCurrencyDataSource(assetPair, "USD000UTSTOM", 0, 1651171835, IntervalType.OneDay)
testingDataSource = MoexCurrencyDataSource(assetPair, "USD000UTSTOM", 1651171835, 1701171835, IntervalType.OneDay)
trainingDataSource.init()
testingDataSource.init()
match config["predictor"]:
    case "base":
        print("DirectPredictor")
        predictor = DirectPredictor(dataSource)
    case "ml":
        print("PercentageMLPredictor")
        predictor = PercentageMLPredictor(config["candlesCount"])
        trainingUseCase = PredictorTrainingUseCase(trainingDataSource, predictor, trainingUseCaseLogger)
        trainingUseCase.execute()
    case "algo":
        print("IndicatorPredictor")
        predictor = IndicatorPredictor(config["candlesCount"])
    case _:
        print("DirectPredictor")
        predictor = DirectPredictor(dataSource)
match config["strategy"]:
    case "direct":
        print("DirectStrategy")
        strategy = DirectStrategy()
    case "base":
        print("DirectStrategy")
        strategy = DirectStrategy()
    case _:
        print("DirectStrategy")
        strategy = DirectStrategy()
contextProvider = DatasetContextProvider(assetPair, dataSource)
market = PortfolioSyncMarket(portfolio, contextProvider)
executor = BackgroundPollingExecutor(market, contextProvider)
match config["assessor"]:
    case "base":
        print("CandleTestAssesor")
        assessor = CandleTestAssesor(portfolio, contextProvider)
    case "TrendFilterAssessor":
        print("TrendFilterAssessor")
        assessor = TrendFilterAssessor(portfolio, contextProvider)
    case "VolatilityThresholdAssessor":
        print("VolatilityThresholdAssessor")
        assessor = VolatilityThresholdAssessor(portfolio, contextProvider, config["params"][0])
    case "RSIFilterAssessor":
        print("RSIFilterAssessor")
        assessor = RSIFilterAssessor(portfolio, contextProvider, config["params"][0], config["params"][1])
    case _:
        print("CandleTestAssesor")
        assessor = CandleTestAssesor(portfolio, contextProvider)

testingUseCase = StrategyTestingUseCase(dataSource, predictor, strategy, assessor, logger, executor, config["iterations"])
testingUseCase.execute()
