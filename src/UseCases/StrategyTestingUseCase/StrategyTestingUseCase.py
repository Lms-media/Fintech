from UseCases import IUseCase
from Entities import TrimmedCandleSeries, INextCandlePrediction
from Interfaces import IDataSource, IPredictor, ILogger

class StrategyTestingUseCase(IUseCase):
    _dataSource: IDataSource
    _logger: ILogger
    _predictor: IPredictor[INextCandlePrediction]

    def __init__(self, dataSource: IDataSource, predictor: IPredictor[INextCandlePrediction], logger: ILogger):
        self._dataSource = dataSource
        self._logger = logger
        self._predictor = predictor

    def execute(self) -> None:
        self._dataSource.init()
        candleSeries = self._dataSource.getSeries()
        totalError = 0
        offset = self._predictor.getCandlesCount()

        for i in range(offset, candleSeries.getCount()):
            trimmedSeries = TrimmedCandleSeries(candleSeries, i - offset, i)
            predicted = self._predictor.predict(trimmedSeries).getNextCandle()
            actual = candleSeries.getByIndex(i - 1)

            if actual:
                delta = predicted.getClosePrice() - actual.getClosePrice()
                error = delta * delta
                totalError += error
                self._logger.log(f"Actual: {actual}")
                self._logger.log(f"Predicted: {predicted}")
                self._logger.log(f"Delta: {delta}")
                self._logger.log(f"Error: {error}")

        relativeError = totalError / (candleSeries.getCount() - offset)

        self._logger.log(f"Total Error: {str(totalError)}")
        self._logger.log(f"Relative Error: {str(relativeError)}")
        self._logger.log(f"Average Delta: {str(relativeError ** 0.5)}")
