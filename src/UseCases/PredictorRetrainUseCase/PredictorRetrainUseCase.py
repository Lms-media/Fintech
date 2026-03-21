from UseCases import IUseCase, PredictorTrainingUseCase
from Entities import TrimmedCandleSeries, INextCandlePrediction
from Interfaces import IDataSource, IPredictor, ILogger
from Contracts import ITrainablePredictor

class PredictorRetrainUseCase(IUseCase):
    _trainingDataSource: IDataSource
    _testingDataSource: IDataSource
    _logger: ILogger
    _predictor: ITrainablePredictor[INextCandlePrediction]

    def __init__(self, trainingDataSource: IDataSource, testingDataSource: IDataSource, predictor: ITrainablePredictor[INextCandlePrediction], logger: ILogger):
        self._trainingDataSource = trainingDataSource
        self._testingDataSource = testingDataSource
        self._logger = logger
        self._predictor = predictor

    def execute(self) -> None:
        self._trainingDataSource.init()
        self._testingDataSource.init()
        trainingCandleSeries = self._trainingDataSource.getSeries()
        testingCandleSeries = self._testingDataSource.getSeries()
        totalError = 0
        offset = self._predictor.getCandlesCount()

        for i in range(offset, trainingCandleSeries.getCount()):
            trimmed = TrimmedCandleSeries(trainingCandleSeries, i - offset, i + 1)
            self._predictor.addDatasetItem(trimmed)

        self._predictor.train(epochs=100)

        for i in range(offset, testingCandleSeries.getCount()):
            if not i == offset:
                oldCandle = testingCandleSeries.getByIndex(i - offset + 1)
                if oldCandle:
                    trainingCandleSeries.appendRight(oldCandle)
                    datasetItem = TrimmedCandleSeries(trainingCandleSeries, trainingCandleSeries.getCount() - offset - 1, trainingCandleSeries.getCount())
                    self._predictor.addDatasetItem(datasetItem)
                    self._predictor.train(epochs=10)

            trimmedTestingSeries = TrimmedCandleSeries(testingCandleSeries, i - offset, i)
            predicted = self._predictor.predict(trimmedTestingSeries).getNextCandle()
            actual = testingCandleSeries.getByIndex(i)

            if actual:
                error = 0
                error += (predicted.getOpenPrice() - actual.getOpenPrice()) ** 2
                error += (predicted.getClosePrice() - actual.getClosePrice()) ** 2
                error += (predicted.getHighPrice() - actual.getHighPrice()) ** 2
                error += (predicted.getLowPrice() - actual.getLowPrice()) ** 2

                totalError += error

                last = testingCandleSeries.getByIndex(i - 1)

                self._logger.log("Last:")
                self._logger.log(str(last))
                self._logger.log("Predicted:")
                self._logger.log(str(predicted))
                self._logger.log("Actual:")
                self._logger.log(str(actual))
                self._logger.log(f"Error: {error}")

        relativeError = totalError / (testingCandleSeries.getCount() - offset)

        self._logger.log(f"Total Error: {str(totalError)}")
        self._logger.log(f"Relative Error: {str(relativeError)}")
