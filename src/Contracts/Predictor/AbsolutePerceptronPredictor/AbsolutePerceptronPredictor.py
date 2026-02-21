from Entities import INextCandlePrediction
from .AbsolutePerceptronPredictorValue import AbsolutePerceptronPredictorValue
from .AbsolutePerceptronPredictorAdapter import AbsolutePerceptronPredictorAdapter
from .AbsolutePerceptronPredictorAlgo import AbsolutePerceptronPredictorAlgo
from ..ATrainablePredictor import ATrainablePredictor
from ..Interfaces import ITrainablePredictor
from Interfaces import ICandleSeries

class AbsolutePerceptronPredictor(ATrainablePredictor[AbsolutePerceptronPredictorValue, INextCandlePrediction], ITrainablePredictor):
    _dataset: list[ICandleSeries]

    def __init__(self, candlesCount: int):
        super().__init__(AbsolutePerceptronPredictorAlgo(candlesCount), AbsolutePerceptronPredictorAdapter(), candlesCount)
