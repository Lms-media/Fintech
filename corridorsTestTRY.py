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


resultFolder = "strategyResult/TRYRUB_TOM_threshold_corridor_up"

stop_coef = 1.0

for assessorIndex in range(10):
    logger = FileLogger(f"logs/strategyTest{assessorIndex}.log")
    trainingUseCaseLogger = FileLogger("logs/trainingUseCase{assessorIndex}.log")

    print("datasource init...")
    testingDataSource = MoexCurrencyDataSource(
        assetPair, "TRYRUB_TOM", 1651171835, 1701171835, IntervalType.OneDay
    )
    testingDataSource.init()
    
    algoPortfolio = RuntimePortfolio(assetPair.getBaseAsset())
    algoPortfolio.deposit(100000)
    algoContextProvider = DatasetContextProvider(assetPair, testingDataSource)
    algoMarket = PortfolioSyncMarket(algoPortfolio, algoContextProvider)
    algoExecutor = BackgroundPollingExecutor(algoMarket, algoContextProvider)
    strategy = DirectStrategy()    
    algoPredictor = IndicatorPredictor(RSIPredictorAlgo(60), 60)
    strategyPrefix = "RSICorridor"
    strategyName = "Стратегия контр-сигнал по RSI с коридором значений"

    capitalizationData.cap.clear()
    assessor = VolatilityCorridorAssessor(algoPortfolio, algoContextProvider, 0.3, stop_coef, 1.0)
    
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
        f"{resultFolder}/algo_top_{stop_coef}Graph.png", dpi=300, bbox_inches="tight"
    )
    capitalizationData.cap.clear()
    stop_coef += 0.1