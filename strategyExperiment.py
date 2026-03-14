import sys
import os
import shutil
import argparse
import json
import matplotlib.pyplot as plt

if os.path.exists('logs'):
    shutil.rmtree('logs')
os.makedirs('logs')

sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from config import assetPair
from UseCases import StrategyTestingUseCase, PredictorTrainingUseCase
from Contracts import DirectPredictor, PercentageMLPredictor, IndicatorPredictor, DummyPredictor
from Services import FileLogger, LoggedPortfolio, RuntimePortfolio, PortfolioSyncMarket, LoggedMarket
from Contracts import MoexCurrencyDataSource, LoggedDataSource, DirectStrategy, CandleTestAssesor, TrendFilterAssessor, VolatilityThresholdAssessor, RSIFilterAssessor, LoggedContextProvider, DatasetContextProvider, BackgroundPollingExecutor, LoggedExecutor
from Interfaces import IntervalType
import capitalizationData

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
mlTestingDataSource = MoexCurrencyDataSource(assetPair, "USD000UTSTOM", 1651784400, 1701171835, IntervalType.OneDay)
trainingDataSource.init()
testingDataSource.init()
mlTestingDataSource.init()

contextProvider = DatasetContextProvider(assetPair, dataSource)
market = PortfolioSyncMarket(portfolio, contextProvider)
executor = BackgroundPollingExecutor(market, contextProvider)

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
    case "dummy":
        print("DummyPredictor")
        predictor = DummyPredictor()
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

testingUseCase = StrategyTestingUseCase(testingDataSource, predictor, strategy, assessor, logger, executor, config["iterations"])
testingUseCase.execute()
print(len(capitalizationData.cap))
percentages = [ (i / 100000) * 100 for i in capitalizationData.cap]
iters = [i for i in range(1, len(capitalizationData.cap) + 1)]


plt.figure(figsize=(12, 6))
plt.plot(iters, percentages, 'g-', marker='o', linewidth=2)
plt.xlabel('Номер итерации', fontsize=12)
plt.ylabel('Процент от исходного значения (%)', fontsize=12)
plt.title('Динамика изменения значений', fontsize=14)
plt.grid(True, linestyle='--', alpha=0.7)
plt.axhline(y=100, color='r', linestyle='-', alpha=0.3, label='100% (исходное)')

for i, p in enumerate(percentages):
    if i % 5 == 0:  # подписываем каждое 5-е значение (чтобы не загромождать)
        plt.text(i, p, f'{p:.1f}%', ha='center', va='bottom', fontsize=8)

plt.legend()
plt.tight_layout()
plt.show()