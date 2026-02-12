from UseCases import IUseCase
from Entities import INextCandlePrediction, TrimmedCandleSeries
from Interfaces import ICandleSeries, IPredictor, ILogger

class PredictorTestingUseCase(IUseCase):
    _candleSeries: ICandleSeries
    _predictor: IPredictor[INextCandlePrediction]
    _logger: ILogger

    def __init__(self, candleSeries: ICandleSeries, predictor: IPredictor[INextCandlePrediction], logger: ILogger):
        self._candleSeries = candleSeries
        self._predictor = predictor
        self._logger = logger

    def execute(self) -> None:
        toIndex = 2
        totalError = 0
        while toIndex < self._candleSeries.getCount() - 1:
            trimmedSeries = TrimmedCandleSeries(self._candleSeries, 0, toIndex)
            predicted = self._predictor.predict(trimmedSeries).getNextCandle()
            toIndex += 1
            actual = self._candleSeries.getByIndex(toIndex)

            delta = predicted.getOpenPrice() - actual.getOpenPrice()
            error = delta * delta
            totalError += error
            self._logger.log(f"Actual:\n{actual}\nPredicted:\n{predicted}")
            self._logger.log(f"Delta: {delta}; Error: {error}")

        self._logger.log(f"Total Error: {totalError}")
