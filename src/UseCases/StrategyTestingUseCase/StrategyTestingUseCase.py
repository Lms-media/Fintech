from UseCases import IUseCase
import time
from Entities import TrimmedCandleSeries, INextCandlePrediction
from Interfaces import IDataSource, IPredictor, ILogger, IStrategy, IAssessor, IExecutor, IAction, ActionStatus

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
        logger: ILogger,
        executor: IExecutor
    ):
        self._dataSource = dataSource
        self._logger = logger
        self._predictor = predictor
        self._strategy = strategy
        self._assessor = assessor
        self._executor = executor

    def execute(self) -> None:
        candleSeries = self._dataSource.getSeries()
        totalError = 0
        offset = self._predictor.getCandlesCount()
        counter = 0
        for i in range(offset, candleSeries.getCount()):
            trimmedSeries = TrimmedCandleSeries(candleSeries, i - offset, i)
            predicted = self._predictor.predict(trimmedSeries)
            signal = self._strategy.getSignal(predicted)
            action: IAction = self._assessor.getAction(signal)
            self._executor.start(action)
            actual = candleSeries.getByIndex(i)
            while action.getStatus() != ActionStatus.Finished:
                time.sleep(0.5)

            if actual:
                delta = predicted.getNextCandle().getClosePrice() - actual.getClosePrice()
                error = delta * delta
                totalError += error
                self._logger.log(f"Actual: {actual}")
                self._logger.log(f"Predicted: {predicted}")
                self._logger.log(f"Delta: {delta}")
                self._logger.log(f"Error: {error}")
                self._logger.log(f"action: {action.__str__()}")
            if counter > 100:
                break
            counter += 1

        relativeError = totalError / (candleSeries.getCount() - offset)

        self._logger.log(f"Total Error: {str(totalError)}")
        self._logger.log(f"Relative Error: {str(relativeError)}")
        self._logger.log(f"Average Delta: {str(relativeError ** 0.5)}")
