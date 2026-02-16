from UseCases import IUseCase
from Interfaces import IDataSource, ILogger
from Entities import TrimmedCandleSeries, INextCandlePrediction
from Contracts import ITrainablePredictor

class PredictorTrainingUseCase(IUseCase):
    _dataSource: IDataSource
    _predictor: ITrainablePredictor[INextCandlePrediction]
    _logger: ILogger

    def __init__(self, dataSource: IDataSource, predictor: ITrainablePredictor[INextCandlePrediction], logger: ILogger):
        self._dataSource = dataSource
        self._predictor = predictor
        self._logger = logger

    def execute(self) -> None:
        self._dataSource.init()
        candleSeries = self._dataSource.getSeries()
        offset = 45

        for i in range(offset, candleSeries.getCount()):
            trimmed = TrimmedCandleSeries(candleSeries, i - offset, i + 1)
            self._predictor.addDatasetItem(trimmed)

        self._predictor.train()
