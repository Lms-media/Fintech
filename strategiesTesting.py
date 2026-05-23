import sys
import os
import shutil
import argparse
import json
import matplotlib.pyplot as plt

if os.path.exists("logs"):
    shutil.rmtree("logs")
os.makedirs("logs")

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

from config import assetPair
from UseCases import StrategyTestingUseCase, PredictorTrainingUseCase
from Contracts import (
    DirectPredictor,
    PercentageMLPredictor,
    IndicatorPredictor,
    DummyPredictor,
    SimpleMAPredictorAlgo,
)
from Services import (
    FileLogger,
    LoggedPortfolio,
    RuntimePortfolio,
    PortfolioSyncMarket,
    LoggedMarket,
)
from Contracts import (
    MoexCurrencyDataSource,
    LoggedDataSource,
    DirectStrategy,
    CandleTestAssesor,
    TrendFilterAssessor,
    VolatilityThresholdAssessor,
    RSIFilterAssessor,
    LoggedContextProvider,
    DatasetContextProvider,
    BackgroundPollingExecutor,
    LoggedExecutor,
)
from Interfaces import IntervalType
import capitalizationData

logger = FileLogger("logs/strategyTest.log")
trainingUseCaseLogger = FileLogger("logs/trainingUseCase.log")

dummyPortfolio = RuntimePortfolio(assetPair.getBaseAsset())
dummyPortfolio.deposit(100000)
algoPortfolio = RuntimePortfolio(assetPair.getBaseAsset())
algoPortfolio.deposit(100000)
mlPortfolio = RuntimePortfolio(assetPair.getBaseAsset())
mlPortfolio.deposit(100000)

print("datasource init...")
trainingDataSource = MoexCurrencyDataSource(
    assetPair, "USD000UTSTOM", 0, 1651171835, IntervalType.OneDay
)
testingDataSource = MoexCurrencyDataSource(
    assetPair, "USD000UTSTOM", 1651171835, 1701171835, IntervalType.OneDay
)
mlTestingDataSource = MoexCurrencyDataSource(
    assetPair, "USD000UTSTOM", 1651784400, 1701171835, IntervalType.OneDay
)
trainingDataSource.init()
testingDataSource.init()
mlTestingDataSource.init()

dummyContextProvider = DatasetContextProvider(assetPair, testingDataSource)
algoContextProvider = DatasetContextProvider(assetPair, testingDataSource)
mlContextProvider = DatasetContextProvider(assetPair, mlTestingDataSource)

dummyMarket = PortfolioSyncMarket(dummyPortfolio, dummyContextProvider)
algoMarket = PortfolioSyncMarket(algoPortfolio, algoContextProvider)
mlMarket = PortfolioSyncMarket(mlPortfolio, mlContextProvider)

dummyExecutor = BackgroundPollingExecutor(dummyMarket, dummyContextProvider)
algoExecutor = BackgroundPollingExecutor(algoMarket, algoContextProvider)
mlExecutor = BackgroundPollingExecutor(mlMarket, mlContextProvider)

strategy = DirectStrategy()

mlPredictor = PercentageMLPredictor(10)
trainingUseCase = PredictorTrainingUseCase(
    trainingDataSource, mlPredictor, trainingUseCaseLogger
)
trainingUseCase.execute()
algoPredictor = IndicatorPredictor(SimpleMAPredictorAlgo(14), 14)
dummyPredictor = DummyPredictor()

# assessor = TrendFilterAssessor(portfolio, contextProvider)
# assessor = VolatilityThresholdAssessor(portfolio, contextProvider, 0.5)
# assessor = RSIFilterAssessor(portfolio, contextProvider, 70, 30)

print("==================Dummy predictor==================")
assessor = VolatilityThresholdAssessor(dummyPortfolio, dummyContextProvider, 0.5)
testingUseCase = StrategyTestingUseCase(
    testingDataSource, dummyPredictor, strategy, assessor, logger, dummyExecutor, 100
)
testingUseCase.execute()
print(len(capitalizationData.cap))
percentages = [(i / 100000) * 100 for i in capitalizationData.cap]
iters = [i for i in range(1, len(capitalizationData.cap) + 1)]

print(f"result: {percentages[-1]}%")
plt.figure(figsize=(12, 6))
plt.plot(iters, percentages, "g-", marker="o", linewidth=2)
plt.xlabel("Итерация", fontsize=12)
plt.ylabel("Процент от исходного капитала", fontsize=12)
plt.title("Стратегия ожидаемая волатильность и некорректный предсказатель", fontsize=14)
plt.grid(True, linestyle="--", alpha=0.7)
plt.axhline(y=100, color="r", linestyle="-", alpha=0.3, label="100% (100000)")

for i, p in enumerate(percentages):
    if i % 5 == 0:
        plt.text(i, p, f"{p:.1f}%", ha="center", va="bottom", fontsize=8)

plt.legend()
plt.tight_layout()
plt.savefig(
    "strategyResult/dummyVolatilityThresholdGraph.png", dpi=300, bbox_inches="tight"
)

print("==================Algo predictor==================")
capitalizationData.cap.clear()
assessor = VolatilityThresholdAssessor(algoPortfolio, algoContextProvider, 0.5)
testingUseCase = StrategyTestingUseCase(
    testingDataSource, algoPredictor, strategy, assessor, logger, algoExecutor, 100
)
testingUseCase.execute()
print(len(capitalizationData.cap))
percentages = [(i / 100000) * 100 for i in capitalizationData.cap]
iters = [i for i in range(1, len(capitalizationData.cap) + 1)]

print(f"result: {percentages[-1]}%")
plt.clf()
plt.figure(figsize=(12, 6))
plt.plot(iters, percentages, "g-", marker="o", linewidth=2)
plt.xlabel("Итерация", fontsize=12)
plt.ylabel("Процент от исходного капитала", fontsize=12)
plt.title(
    "Стратегия ожидаемая волатильность и алгоритмический предсказатель", fontsize=14
)
plt.grid(True, linestyle="--", alpha=0.7)
plt.axhline(y=100, color="r", linestyle="-", alpha=0.3, label="100% (100000)")

for i, p in enumerate(percentages):
    if i % 5 == 0:
        plt.text(i, p, f"{p:.1f}%", ha="center", va="bottom", fontsize=8)

plt.legend()
plt.tight_layout()
plt.savefig(
    "strategyResult/algoVolatilityThresholdGraph.png", dpi=300, bbox_inches="tight"
)

print("==================Ml predictor==================")
capitalizationData.cap.clear()
assessor = VolatilityThresholdAssessor(mlPortfolio, mlContextProvider, 0.5)
testingUseCase = StrategyTestingUseCase(
    mlTestingDataSource, mlPredictor, strategy, assessor, logger, mlExecutor, 100
)
testingUseCase.execute()
print(len(capitalizationData.cap))
percentages = [(i / 100000) * 100 for i in capitalizationData.cap]
iters = [i for i in range(1, len(capitalizationData.cap) + 1)]

print(f"result: {percentages[-1]}%")
plt.clf()
plt.figure(figsize=(12, 6))
plt.plot(iters, percentages, "g-", marker="o", linewidth=2)
plt.xlabel("Итерация", fontsize=12)
plt.ylabel("Процент от исходного капитала", fontsize=12)
plt.title(
    "Стратегия ожидаемая волатильность и предсказатель на основе машинного обучения",
    fontsize=14,
)
plt.grid(True, linestyle="--", alpha=0.7)
plt.axhline(y=100, color="r", linestyle="-", alpha=0.3, label="100% (100000)")

for i, p in enumerate(percentages):
    if i % 5 == 0:
        plt.text(i, p, f"{p:.1f}%", ha="center", va="bottom", fontsize=8)

plt.legend()
plt.tight_layout()
plt.savefig(
    "strategyResult/mlVolatilityThresholdGraph.png", dpi=300, bbox_inches="tight"
)
