from UseCases import IUseCase
import time
from Entities import INextCandlePrediction
from Interfaces import IDataSource, IPredictor, ILogger, IStrategy, IAssessor, IExecutor, IAction, ActionStatus
from UseCases.predictionInput import prediction_input
import warnings

class StrategyTestingUseCase(IUseCase):
    _dataSource: IDataSource
    _logger: ILogger
    _predictor: IPredictor[INextCandlePrediction]
    _iter_count: int

    def __init__(
        self, 
        dataSource: IDataSource, 
        predictor: IPredictor[INextCandlePrediction],
        strategy: IStrategy,
        assessor: IAssessor,
        logger: ILogger,
        executor: IExecutor,
        iterCount: int,
        startIndex=None,
        actionTimeout: float = 30.0,
    ):
        self._dataSource = dataSource
        self._logger = logger
        self._predictor = predictor
        self._strategy = strategy
        self._assessor = assessor
        self._executor = executor
        self._iter_count = iterCount
        self._startIndex = startIndex
        self._actionTimeout = actionTimeout
        if iterCount < 1 or actionTimeout <= 0:
            raise ValueError("Positive iteration count and timeout required")

    def execute(self) -> dict:
        if not self._predictor.usesCurrentOpen():
            warnings.warn("Legacy timing: assessors may trade a candle whose close was already observed. Use OpenCloseStrategy with an opening-time predictor and CorridorAssessor.", RuntimeWarning)
        candleSeries = self._dataSource.getSeries()
        totalError = 0
        offset = self._predictor.getCandlesCount()
        start = offset if self._startIndex is None else self._startIndex
        if not offset <= start < candleSeries.getCount():
            raise ValueError("Testing range must contain at least one candle after warmup")
        counter = 0
        self.records = []
        for i in range(start, min(candleSeries.getCount(), start + self._iter_count)):
            trimmedSeries = prediction_input(self._predictor, candleSeries, i)
            predicted = self._predictor.predict(trimmedSeries)
            signal = self._strategy.getSignal(predicted)
            action: IAction = self._assessor.getAction(signal)
            if self._predictor.usesCurrentOpen():
                from Entities.Action.CandleCorridorAction import CandleCorridorAction
                if not isinstance(action, CandleCorridorAction):
                    raise ValueError("Opening-time backtest requires a current-candle corridor action")
            self._executor.start(action)
            actual = candleSeries.getByIndex(i)
            deadline = time.monotonic() + self._actionTimeout
            while action.getStatus() != ActionStatus.Finished:
                if time.monotonic() >= deadline:
                    raise TimeoutError("Action did not complete; check the executor for errors")
                time.sleep(0.01)

            if actual:
                delta = predicted.getNextCandle().getClosePrice() - actual.getClosePrice()
                error = delta * delta
                totalError += error
                self._logger.log(f"Actual: {actual}")
                self._logger.log(f"Predicted: {predicted}")
                self._logger.log(f"Delta: {delta}")
                self._logger.log(f"Error: {error}")
                self._logger.log(f"action: {action.__str__()}")
                self.records.append(dict(timestamp=actual.getOpenTimestamp(),
                                         predictedClose=predicted.getNextCandle().getClosePrice(),
                                         actualClose=actual.getClosePrice(),
                                         lots=action.getTasks()[0].getLotCount() if action.getTasks() else 0))
            counter += 1

        relativeError = totalError / counter

        self._logger.log(f"Total Error: {str(totalError)}")
        self._logger.log(f"Relative Error: {str(relativeError)}")
        self._logger.log(f"Average Delta: {str(relativeError ** 0.5)}")
        self.result = dict(count=counter, closeMSE=relativeError, closeRMSE=relativeError ** 0.5)
        return self.result
