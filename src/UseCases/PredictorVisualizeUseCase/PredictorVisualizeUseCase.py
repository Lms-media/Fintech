from UseCases import IUseCase
from Interfaces import IDataSource, IPredictor, ILogger
from Entities import TrimmedCandleSeries, INextCandlePrediction

class PredictorVisualizeUseCase(IUseCase):
    _dataSource: IDataSource
    _predictor: IPredictor[INextCandlePrediction]
    _logger: ILogger

    def __init__(self, dataSource: IDataSource, predictor: IPredictor[INextCandlePrediction], logger: ILogger):
        self._dataSource = dataSource
        self._predictor = predictor
        self._logger = logger

    def execute(self) -> None:
        self._dataSource.init()
        self._visualize()

    def _visualize(self) -> None:
        candleSeries = self._dataSource.getSeries()

        for i in range(candleSeries.getCount()):
            actual = candleSeries.getByIndex(i)

            if not actual:
                continue

            self._logger.log(f"x:{i};actual:{actual.getClosePrice()}")

            if i < 45:
                continue

            trimmed = TrimmedCandleSeries(candleSeries, 0, i)
            prediction = self._predictor.predict(trimmed)
            predicted = prediction.getNextCandle()
            self._logger.log(f"x:{i};predicted:{predicted.getClosePrice()}")
