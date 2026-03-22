import sys
import os
import shutil

if os.path.exists('logs'):
    shutil.rmtree('logs')

os.makedirs('logs')

sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from config import assetPair

from UseCases import PredictorVisualizeUseCase, PredictorTrainingUseCase, PredictorTestingUseCase, PredictorRetrainUseCase
from Contracts import (
    MoexCurrencyDataSource,
    LoggedDataSource,
    IndicatorPredictor,
    CompositeMedianPredictor,
    CompositeVotingPredictor,
    CompositeAvgPredictor,
    ExponentialMAPredictorAlgo,
    BollingerBandsPredictorAlgo,
    RSIPredictorAlgo,
    MACDPredictorAlgo,
    PercentageMLPredictor,
    PercentageDeltaMLPredictor,
)
from Services import FileLogger, GraphLogger2D
from Interfaces import IntervalType

trainingDataSourceLogger = FileLogger("logs/trainingDataSource.log")
testingDataSourceLogger = FileLogger("logs/testingDataSource.log")
predictorLogger = FileLogger("logs/predictor.log")
mainLogger = FileLogger("logs/main.log")
dynamicLogger = GraphLogger2D("logs/dynamic.png")
testingLogger = FileLogger("logs/testing.log")
trainingDataSource = LoggedDataSource(MoexCurrencyDataSource(assetPair, "EURUSD000TOM", 0, 1651171835, IntervalType.OneDay), trainingDataSourceLogger)
testingDataSource = LoggedDataSource(MoexCurrencyDataSource(assetPair, "EURUSD000TOM", 1651171835, 1701171835, IntervalType.OneDay), testingDataSourceLogger)
# predictor = RelativeMLPredictor(45)

i = 55
logger = FileLogger(f"logs/{i}.log")
predictor = PercentageDeltaMLPredictor(i)

trainingDataSource.init()
print(trainingDataSource.getSeries().getCount())

# trainingUseCase = PredictorTrainingUseCase(trainingDataSource, predictor, mainLogger)
# trainingUseCase.execute()

# testingUseCase = PredictorTestingUseCase(testingDataSource, predictor, logger)
# testingUseCase.execute()

useCase = PredictorRetrainUseCase(trainingDataSource, testingDataSource, predictor, logger)
useCase.execute()

# trainingUseCase = PredictorTrainingUseCase(trainingDataSource, predictor, mainLogger)
# trainingUseCase.execute()

# visualizeUseCase = PredictorVisualizeUseCase(testingDataSource, predictor, dynamicLogger)
# visualizeUseCase.execute()

# testingUseCase = PredictorTestingUseCase(testingDataSource, predictor, testingLogger)
# testingUseCase.execute()
