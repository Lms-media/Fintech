from Entities import INextCandlePrediction
from .MLPredictorValue import MLPredictorValue
from .MLPredictorAdapter import MLPredictorAdapter
from .MLPredictorAlgo import MLPredictorAlgo
from ..ATrainablePredictor import ATrainablePredictor
from ..Interfaces import ITrainablePredictor
from Interfaces import ICandleSeries

class MLPredictor(ATrainablePredictor[MLPredictorValue, INextCandlePrediction], ITrainablePredictor):
    _dataset: list[ICandleSeries]

    def __init__(self, candlesCount: int):
        super().__init__(MLPredictorAlgo(candlesCount), MLPredictorAdapter(), candlesCount)
