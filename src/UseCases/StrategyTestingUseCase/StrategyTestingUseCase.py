from UseCases import IUseCase
from Entities import TrimmedCandleSeries, INextCandlePrediction
from Interfaces import IDataSource, IPredictor, ILogger, IStrategy, IAssessor

class StrategyTestingUseCase(IUseCase):
    _dataSource: IDataSource
    _logger: ILogger
    _predictor: IPredictor[INextCandlePrediction]

    def __init__(
        self, 
        dataSource: IDataSource, 
        predictor: IPredictor[INextCandlePrediction],
        strategy: IStrategy,
        assessor: IAssessor,
        logger: ILogger
    ):
        self._dataSource = dataSource
        self._logger = logger
        self._predictor = predictor
        self._strategy = strategy
        self._assessor = assessor

    def execute(self) -> None:
        candleSeries = self._dataSource.getSeries()
        totalError = 0
        offset = self._predictor.getCandlesCount()

        for i in range(offset, candleSeries.getCount()):
            trimmedSeries = TrimmedCandleSeries(candleSeries, i - offset, i)
            predicted = self._predictor.predict(trimmedSeries)
            signal = self._strategy.getSignal(predicted)
            action = self._assessor.getAction(signal)
            actual = candleSeries.getByIndex(i)

            if actual:
                delta = predicted.getNextCandle().getClosePrice() - actual.getClosePrice()
                error = delta * delta
                totalError += error
                self._logger.log(f"Actual: {actual}")
                self._logger.log(f"Predicted: {predicted}")
                self._logger.log(f"Delta: {delta}")
                self._logger.log(f"Error: {error}")
                self._logger.log(f"action: {action.__str__()}")

        relativeError = totalError / (candleSeries.getCount() - offset)

        self._logger.log(f"Total Error: {str(totalError)}")
        self._logger.log(f"Relative Error: {str(relativeError)}")
        self._logger.log(f"Average Delta: {str(relativeError ** 0.5)}")
