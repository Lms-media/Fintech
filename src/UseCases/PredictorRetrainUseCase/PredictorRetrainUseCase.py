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

        retrainInterval = 50
        stepsSinceRetrain = 0

        for i in range(offset, testingCandleSeries.getCount()):
            if not i == offset:
                oldCandle = testingCandleSeries.getByIndex(i - offset + 1)
                if oldCandle:
                    trainingCandleSeries.appendRight(oldCandle)
                    datasetItem = TrimmedCandleSeries(trainingCandleSeries, trainingCandleSeries.getCount() - offset - 1, trainingCandleSeries.getCount())
                    self._predictor.addDatasetItem(datasetItem)
                    stepsSinceRetrain += 1

                    if stepsSinceRetrain >= retrainInterval:
                        self._predictor.train(epochs=10, datasetItemLimit=500)
                        stepsSinceRetrain = 0

            trimmedTestingSeries = TrimmedCandleSeries(testingCandleSeries, i - offset, i)
            predicted = self._predictor.predict(trimmedTestingSeries).getNextCandle()
            actual = testingCandleSeries.getByIndex(i)

            if actual:
                error = 0

                openPriceError = ((predicted.getOpenPrice() - actual.getOpenPrice()) / actual.getOpenPrice()) ** 2
                error += openPriceError

                closePriceError = ((predicted.getClosePrice() - actual.getClosePrice()) / actual.getClosePrice()) ** 2
                error += closePriceError

                highPriceError = ((predicted.getHighPrice() - actual.getHighPrice()) / actual.getHighPrice()) ** 2
                error += highPriceError

                lowPriceError = ((predicted.getLowPrice() - actual.getLowPrice()) / actual.getLowPrice()) ** 2
                error += lowPriceError

                totalError += error

                last = testingCandleSeries.getByIndex(i - 1)

                self._logger.log(f"Error: {error}")

        relativeError = totalError / (testingCandleSeries.getCount() - offset)

        self._logger.log(f"Total Error: {str(totalError)}")
        self._logger.log(f"Relative Error: {str(relativeError)}")
        self._logger.log(f"Relative Scaled Error: {str(relativeError * 10000)}")
