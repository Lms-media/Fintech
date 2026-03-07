from UseCases import IUseCase
from Entities import TrimmedCandleSeries, INextCandlePrediction
from Interfaces import IDataSource, IPredictor, ILogger

class PredictorTestingUseCase(IUseCase):
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
            actual = candleSeries.getByIndex(i)

            if actual:
                error = 0
                error += (predicted.getOpenPrice() - actual.getOpenPrice()) ** 2
                error += (predicted.getClosePrice() - actual.getClosePrice()) ** 2
                error += (predicted.getHighPrice() - actual.getHighPrice()) ** 2
                error += (predicted.getLowPrice() - actual.getLowPrice()) ** 2

                totalError += error

                self._logger.log(f"Error: {error}")

        relativeError = totalError / (candleSeries.getCount() - offset)

        self._logger.log(f"Total Error: {str(totalError)}")
        self._logger.log(f"Relative Error: {str(relativeError)}")
