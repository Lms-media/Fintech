import sys
import os
import shutil

if os.path.exists('logs'):
    shutil.rmtree('logs')
os.makedirs('logs')

sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from config import assetPair

from UseCases import PredictorVisualizeUseCase, PredictorTrainingUseCase, PredictorTestingUseCase
from Contracts import MoexCurrencyDataSource, LoggedDataSource, PercentageMLPredictor, MAPredictor, DummyPredictor, AbsolutePerceptronPredictor
from Services import FileLogger, GraphLogger2D
from Interfaces import IntervalType

trainingDataSourceLogger = FileLogger("logs/trainingDataSource.log")
testingDataSourceLogger = FileLogger("logs/testingDataSource.log")
predictorLogger = FileLogger("logs/predictor.log")
mainLogger = FileLogger("logs/main.log")
dynamicLogger = GraphLogger2D("logs/dynamic.png")
testingLogger = FileLogger("logs/testing.log")
trainingDataSource = LoggedDataSource(MoexCurrencyDataSource(assetPair, "USD000UTSTOM", 0, 1651171835, IntervalType.OneDay), trainingDataSourceLogger)
testingDataSource = LoggedDataSource(MoexCurrencyDataSource(assetPair, "USD000UTSTOM", 1651171835, 1701171835, IntervalType.OneDay), testingDataSourceLogger)
# predictor = RelativeMLPredictor(45)

for i in range(1, 101):
    print(i)
    logger = FileLogger(f"logs/{i}.log")
    predictor = PercentageMLPredictor(i)

    trainingUseCase = PredictorTrainingUseCase(trainingDataSource, predictor, mainLogger)
    trainingUseCase.execute()

    testingUseCase = PredictorTestingUseCase(testingDataSource, predictor, logger)
    testingUseCase.execute()

# trainingUseCase = PredictorTrainingUseCase(trainingDataSource, predictor, mainLogger)
# trainingUseCase.execute()

# visualizeUseCase = PredictorVisualizeUseCase(testingDataSource, predictor, dynamicLogger)
# visualizeUseCase.execute()

# testingUseCase = PredictorTestingUseCase(testingDataSource, predictor, testingLogger)
# testingUseCase.execute()
