import sys
import os
import shutil
import matplotlib.pyplot as plt

if os.path.exists("logs"):
    shutil.rmtree("logs")
os.makedirs("logs")

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

from UseCases import StrategyTestingUseCase, PredictorTrainingUseCase
from Contracts import (
    IndicatorPredictor,
    DummyPredictor,
)
from Services import (
    FileLogger,
    RuntimePortfolio,
    PortfolioSyncMarket,
)
from Contracts import (
    MoexCurrencyDataSource,
    DirectStrategy,
    TrendFilterAssessor,
    VolatilityThresholdAssessor,
    RSIFilterAssessor,
    VolatilityCorridorAssessor,
    RSICorridorAssessor,
    DatasetContextProvider,
    BackgroundPollingExecutor,
    PercentageDeltaMLPredictor,
    RSIPredictorAlgo
)
from Interfaces import IntervalType
import capitalizationData
from ValueObjects import Asset, AssetPair

baseAsset = Asset('RUB', 1)
# KZT
# TRY
# USD
quoteAsset = Asset('TRY', 1)
assetPair = AssetPair(baseAsset, quoteAsset)
resultFolder = "strategyResult/TRYRUB_TOM_results_final"

for assessorIndex in range(5):
    logger = FileLogger(f"logs/strategyTest{assessorIndex}.log")
    trainingUseCaseLogger = FileLogger("logs/trainingUseCase{assessorIndex}.log")

    print("datasource init...")
    trainingDataSource = MoexCurrencyDataSource(
        assetPair, "TRYRUB_TOM", 0, 1651171835, IntervalType.OneDay
    )
    testingDataSource = MoexCurrencyDataSource(
        assetPair, "TRYRUB_TOM", 1651171835, 1701171835, IntervalType.OneDay
    )
    mlTestingDataSource = MoexCurrencyDataSource(
        assetPair, "TRYRUB_TOM", 1651171835, 1701171835, IntervalType.OneDay
    )
    trainingDataSource.init()
    testingDataSource.init()
    mlTestingDataSource.init()
    
    dummyPortfolio = RuntimePortfolio(assetPair.getBaseAsset())
    dummyPortfolio.deposit(100000)
    algoPortfolio = RuntimePortfolio(assetPair.getBaseAsset())
    algoPortfolio.deposit(100000)
    mlPortfolio = RuntimePortfolio(assetPair.getBaseAsset())
    mlPortfolio.deposit(100000)
    
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

    mlPredictor = PercentageDeltaMLPredictor(60)
    trainingUseCase = PredictorTrainingUseCase(
        trainingDataSource, mlPredictor, trainingUseCaseLogger
    )
    trainingUseCase.execute()
    algoPredictor = IndicatorPredictor(RSIPredictorAlgo(60), 60)
    dummyPredictor = DummyPredictor()

    print("==================Dummy predictor==================")
    capitalizationData.cap.clear()
    assessor = VolatilityThresholdAssessor(dummyPortfolio, dummyContextProvider, 0.3)
    strategyPrefix = "VolatilityThreshold"
    strategyName = "Стратегия ожидаемая волатильность"
    if assessorIndex == 0:
        assessor = VolatilityThresholdAssessor(dummyPortfolio, dummyContextProvider, 0.3)
        strategyPrefix = "VolatilityThreshold"
        strategyName = "Стратегия ожидаемая волатильность"
    elif assessorIndex == 1:
        assessor = TrendFilterAssessor(dummyPortfolio, dummyContextProvider)
        strategyPrefix = "TrendFilter"
        strategyName = "Стратегия фильтрация по тренду"
    elif assessorIndex == 2:
        assessor = RSIFilterAssessor(dummyPortfolio, dummyContextProvider, 70, 30)
        strategyPrefix = "RSIFilter"
        strategyName = "Стратегия контр-сигнал по RSI"
    elif assessorIndex == 3:
        assessor = VolatilityCorridorAssessor(dummyPortfolio, dummyContextProvider, 0.3, 0.4, 1.0)
        strategyPrefix = "VolatilityCorridor"
        strategyName = "Стратегия ожидаемая волатильность с коридором значений"
    elif assessorIndex == 4:
        assessor = RSICorridorAssessor(dummyPortfolio, dummyContextProvider, 70, 30, 0.5, 1.95)
        strategyPrefix = "RSICorridor"
        strategyName = "Стратегия контр-сигнал по RSI с коридором значений"
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
    plt.title(f"{strategyName} и некорректный предсказатель", fontsize=14)
    plt.grid(True, linestyle="--", alpha=0.7)
    plt.axhline(y=100, color="r", linestyle="-", alpha=0.3, label="100% (100000)")

    for i, p in enumerate(percentages):
        if i % 5 == 0:
            plt.text(i, p, f"{p:.1f}%", ha="center", va="bottom", fontsize=8)

    plt.legend()
    plt.tight_layout()
    plt.savefig(
        f"{resultFolder}/dummy{strategyPrefix}Graph.png", dpi=300, bbox_inches="tight"
    )
    capitalizationData.cap.clear()

    print("==================Algo predictor==================")
    capitalizationData.cap.clear()
    assessor = VolatilityThresholdAssessor(algoPortfolio, algoContextProvider, 0.3)
    if assessorIndex == 0:
        assessor = VolatilityThresholdAssessor(algoPortfolio, algoContextProvider, 0.3)
    elif assessorIndex == 1:
        assessor = TrendFilterAssessor(algoPortfolio, algoContextProvider)
    elif assessorIndex == 2:
        assessor = RSIFilterAssessor(algoPortfolio, algoContextProvider, 70, 30)
    elif assessorIndex == 3:
        assessor = VolatilityCorridorAssessor(algoPortfolio, algoContextProvider, 0.3, 0.4, 1.0)
    elif assessorIndex == 4:
        assessor = RSICorridorAssessor(algoPortfolio, algoContextProvider, 70, 30, 0.5, 1.95)
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
        f"{strategyName} и алгоритмический предсказатель", fontsize=14
    )
    plt.grid(True, linestyle="--", alpha=0.7)
    plt.axhline(y=100, color="r", linestyle="-", alpha=0.3, label="100% (100000)")

    for i, p in enumerate(percentages):
        if i % 5 == 0:
            plt.text(i, p, f"{p:.1f}%", ha="center", va="bottom", fontsize=8)

    plt.legend()
    plt.tight_layout()
    plt.savefig(
        f"{resultFolder}/algo{strategyPrefix}Graph.png", dpi=300, bbox_inches="tight"
    )
    capitalizationData.cap.clear()

    print("==================Ml predictor==================")
    capitalizationData.cap.clear()
    assessor = VolatilityThresholdAssessor(mlPortfolio, mlContextProvider, 0.3)
    if assessorIndex == 0:
        assessor = VolatilityThresholdAssessor(mlPortfolio, mlContextProvider, 0.3)
    elif assessorIndex == 1:
        assessor = TrendFilterAssessor(mlPortfolio, mlContextProvider)
    elif assessorIndex == 2:
        assessor = RSIFilterAssessor(mlPortfolio, mlContextProvider, 70, 30)
    elif assessorIndex == 3:
        assessor = VolatilityCorridorAssessor(mlPortfolio, mlContextProvider, 0.3, 0.4, 1.0)
    elif assessorIndex == 4:
        assessor = RSICorridorAssessor(mlPortfolio, mlContextProvider, 70, 30, 0.5, 1.95)
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
        f"{strategyName} и предсказатель на основе машинного обучения",
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
        f"{resultFolder}/ml{strategyPrefix}Graph.png", dpi=300, bbox_inches="tight"
    )
    capitalizationData.cap.clear()